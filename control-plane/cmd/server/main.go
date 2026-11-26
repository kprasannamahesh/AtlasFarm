package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/api"
	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/broker"
	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/dispatch"
	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/events"
	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/lease"
	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/rollout"
	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/telemetry"
	"github.com/kprasannamahesh/AtlasFarm/scheduler"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func main() {
	dsn := env("DATABASE_DSN", "root:atlasfarm-local@tcp(127.0.0.1:3306)/atlasfarm?parseTime=true")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("open MySQL: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)
	startupCtx, startupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := db.PingContext(startupCtx); err != nil {
		startupCancel()
		log.Fatalf("connect to MySQL: %v", err)
	}
	startupCancel()

	eventStore := events.New(db)
	rolloutStore := rollout.New(db)
	migrationCtx, migrationCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := eventStore.Migrate(migrationCtx); err != nil {
		migrationCancel()
		log.Fatalf("migrate event store: %v", err)
	}
	if err := rolloutStore.Migrate(migrationCtx); err != nil {
		migrationCancel()
		log.Fatalf("migrate rollout store: %v", err)
	}
	migrationCancel()

	traceCtx, traceCancel := context.WithTimeout(context.Background(), 10*time.Second)
	shutdownTracing, err := telemetry.Setup(traceCtx, "atlasfarm-control-plane")
	traceCancel()
	if err != nil {
		log.Fatalf("initialize OpenTelemetry: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := shutdownTracing(ctx); err != nil {
			log.Printf("shutdown OpenTelemetry: %v", err)
		}
	}()

	queue := env("RABBITMQ_QUEUE", "jobs")
	publisher, rabbitConn, err := broker.Connect(env("RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:25672/"), queue)
	if err != nil {
		log.Fatalf("connect to RabbitMQ: %v", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			log.Printf("close RabbitMQ channel: %v", err)
		}
		if err := rabbitConn.Close(); err != nil {
			log.Printf("close RabbitMQ connection: %v", err)
		}
	}()

	packer, err := scheduler.NewBinPackerFromEnv()
	if err != nil {
		log.Fatalf("configure scheduler: %v", err)
	}
	cluster := scheduler.NewClusterState()
	dispatcher := dispatch.New(eventStore, publisher, cluster, packer, rolloutStore)
	reaper := lease.NewReaper(eventStore, cluster, lease.DefaultMaxRetries)
	rolloutWatcher := rollout.NewWatcher(rolloutStore)

	mux := http.NewServeMux()
	apiHandler := api.NewHandlerWithRollouts(eventStore, publisher, rolloutStore, cluster)
	apiRoutes := apiHandler.Routes()
	mux.Handle("/v1/", apiRoutes)
	mux.Handle("/api/v1/", http.StripPrefix("/api", apiRoutes))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		if rabbitConn.IsClosed() || publisher.IsClosed() {
			http.Error(w, "message broker unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte("# HELP atlasfarm_control_plane_up Control plane process status.\n# TYPE atlasfarm_control_plane_up gauge\natlasfarm_control_plane_up 1\n"))
		_, _ = w.Write([]byte(cluster.Utilization().Prometheus()))
		_, _ = fmt.Fprintf(w, "# HELP atlasfarm_stale_commits_rejected_total Lifecycle callbacks rejected for stale execution tokens.\n# TYPE atlasfarm_stale_commits_rejected_total counter\natlasfarm_stale_commits_rejected_total %d\n", apiHandler.StaleCommitsRejected())
	})

	server := &http.Server{
		Addr:              env("HTTP_ADDR", ":8080"),
		Handler:           otelhttp.NewHandler(withConsoleCORS(mux), "atlasfarm-control-plane"),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go dispatcher.Run(ctx)
	go reaper.Run(ctx)
	go rolloutWatcher.Run(ctx)

	serverErrors := make(chan error, 1)
	go func() {
		log.Printf("control plane listening on %s", server.Addr)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve control plane: %v", err)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Fatalf("shutdown control plane: %v", err)
		}
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func withConsoleCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "http://localhost:5173" || origin == "http://127.0.0.1:5173" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Add("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Idempotency-Key")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
