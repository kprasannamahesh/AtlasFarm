package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kprasannamahesh/AtlasFarm/worker-agent/internal/agent"
)

func main() {
	cpu := flag.Int("cpu", 1, "available CPU cores")
	memory := flag.Int("memory", 1024, "available memory in MiB")
	gpu := flag.Int("gpu", 0, "available GPUs")
	version := flag.String("version", "dev", "worker runner version")
	id := flag.String("id", "", "worker ID (defaults to hostname and process ID)")
	controlPlane := flag.String("control-plane", "http://127.0.0.1:8080", "control-plane base URL")
	rabbitMQ := flag.String("rabbitmq-url", "amqp://guest:guest@127.0.0.1:5672/", "RabbitMQ URL")
	queue := flag.String("queue", "jobs", "RabbitMQ job queue")
	concurrency := flag.Int("concurrency", 1, "maximum concurrent job executions")
	jobTimeout := flag.Duration("job-timeout", 15*time.Minute, "maximum duration for one job")
	heartbeatInterval := flag.Duration("heartbeat-interval", 5*time.Second, "worker heartbeat interval")
	metricsAddr := flag.String("metrics-addr", ":9091", "worker metrics listen address")
	flag.Parse()

	if *cpu < 1 || *memory < 1 || *gpu < 0 || *concurrency < 1 ||
		*jobTimeout <= 0 || *heartbeatInterval <= 0 {
		log.Fatal("require cpu >= 1, memory >= 1, gpu >= 0, concurrency >= 1, and positive timeout/heartbeat intervals")
	}
	workerID := *id
	if workerID == "" {
		workerID = fmt.Sprintf("%s-%d", agent.Hostname(), os.Getpid())
		if len(workerID) > 36 {
			workerID = workerID[:36]
		}
	}
	if len(workerID) > 36 {
		log.Fatal("worker ID must be at most 36 characters")
	}
	config := agent.Config{
		ID: workerID, ControlPlaneURL: *controlPlane, RabbitMQURL: *rabbitMQ,
		Queue: *queue, CPU: *cpu, MemoryMB: int64(*memory), GPU: *gpu,
		RunnerVersion: *version, Concurrency: *concurrency,
		JobTimeout: *jobTimeout, HeartbeatInterval: *heartbeatInterval,
	}
	log.Printf("worker agent starting: id=%s cpu=%d memory_mb=%d gpu=%d concurrency=%d version=%s",
		workerID, *cpu, *memory, *gpu, *concurrency, *version)

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = fmt.Fprintf(w, "# HELP atlasfarm_worker_resources Worker resource capacity.\n# TYPE atlasfarm_worker_resources gauge\natlasfarm_worker_resources{resource=\"cpu\"} %d\natlasfarm_worker_resources{resource=\"memory_mb\"} %d\natlasfarm_worker_resources{resource=\"gpu\"} %d\n", *cpu, *memory, *gpu)
	})
	server := &http.Server{
		Addr:              *metricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverErrors := make(chan error, 1)
	go func() {
		log.Printf("worker metrics listening on %s", server.Addr)
		serverErrors <- server.ListenAndServe()
	}()
	agentErrors := make(chan error, 1)
	go func() {
		agentErrors <- agent.New(config).Run(ctx)
	}()

	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			stop()
			log.Fatalf("serve worker metrics: %v", err)
		}
	case err := <-agentErrors:
		if err != nil && !errors.Is(err, context.Canceled) {
			stop()
			log.Fatalf("run worker agent: %v", err)
		}
	case <-ctx.Done():
	}
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("shutdown worker metrics: %v", err)
	}
}
