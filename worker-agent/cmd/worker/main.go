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
)

func main() {
	cpu := flag.Int("cpu", 1, "available CPU cores")
	memory := flag.Int("memory", 1024, "available memory in MiB")
	gpu := flag.Int("gpu", 0, "available GPUs")
	version := flag.String("version", "dev", "worker runner version")
	flag.Parse()

	if *cpu < 1 || *memory < 1 || *gpu < 0 {
		log.Fatal("resource flags require --cpu >= 1, --memory >= 1, and --gpu >= 0")
	}

	log.Printf("worker agent starting: cpu=%d memory_mb=%d gpu=%d version=%s", *cpu, *memory, *gpu, *version)

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = fmt.Fprintf(w, "# HELP atlasfarm_worker_resources Worker resource capacity.\n# TYPE atlasfarm_worker_resources gauge\natlasfarm_worker_resources{resource=\"cpu\"} %d\natlasfarm_worker_resources{resource=\"memory_mb\"} %d\natlasfarm_worker_resources{resource=\"gpu\"} %d\n", *cpu, *memory, *gpu)
	})
	server := &http.Server{
		Addr:              ":9091",
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

	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve worker metrics: %v", err)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Fatalf("shutdown worker metrics: %v", err)
		}
	}
}
