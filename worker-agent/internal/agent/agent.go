package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/kprasannamahesh/AtlasFarm/worker-agent/internal/executor"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Config struct {
	ID                string
	ControlPlaneURL   string
	RabbitMQURL       string
	Queue             string
	CPU               int
	MemoryMB          int64
	GPU               int
	RunnerVersion     string
	Concurrency       int
	JobTimeout        time.Duration
	HeartbeatInterval time.Duration
}

type Job struct {
	ID          string `json:"id"`
	Command     string `json:"command"`
	Status      string `json:"status"`
	WorkerID    string `json:"worker_id"`
	ExecutionID int64  `json:"execution_id"`
	Claimed     bool   `json:"claimed"`
}

type ActiveLease struct {
	JobID       string `json:"job_id"`
	ExecutionID int64  `json:"execution_id"`
}

type Agent struct {
	config Config
	client *http.Client
	mu     sync.RWMutex
	active map[string]int64
}

func New(config Config) *Agent {
	return &Agent{
		config: config,
		client: &http.Client{Timeout: 10 * time.Second},
		active: make(map[string]int64),
	}
}

func (a *Agent) Run(ctx context.Context) error {
	connection, err := amqp.Dial(a.config.RabbitMQURL)
	if err != nil {
		return fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("open RabbitMQ channel: %w", err)
	}
	defer channel.Close()
	exchange := "atlasfarm.jobs"
	if err := channel.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare job exchange: %w", err)
	}
	routingKey := "jobs.worker." + a.config.ID
	queueName := routingKey
	if _, err := channel.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare job queue: %w", err)
	}
	if err := channel.QueueBind(queueName, routingKey, exchange, false, nil); err != nil {
		return fmt.Errorf("bind worker job queue: %w", err)
	}
	if err := channel.Qos(a.config.Concurrency, 0, false); err != nil {
		return fmt.Errorf("configure job prefetch: %w", err)
	}
	deliveries, err := channel.Consume(queueName, a.config.ID, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume job queue: %w", err)
	}
	if err := a.request(ctx, http.MethodPost, "/v1/workers", map[string]any{
		"id": a.config.ID, "cpu": a.config.CPU, "memory_mb": a.config.MemoryMB,
		"gpu": a.config.GPU, "runner_version": a.config.RunnerVersion,
	}, nil); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errorsFromWorkers := make(chan error, 1)
	var workers sync.WaitGroup
	for i := 0; i < a.config.Concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for delivery := range deliveries {
				if err := a.execute(runCtx, delivery); err != nil {
					select {
					case errorsFromWorkers <- err:
					default:
					}
					cancel()
					return
				}
			}
		}()
	}
	workersDone := make(chan struct{})
	go func() {
		workers.Wait()
		close(workersDone)
	}()

	heartbeatErrors := make(chan error, 1)
	go a.heartbeatLoop(runCtx, heartbeatErrors)

	select {
	case <-ctx.Done():
		cancel()
		if err := channel.Cancel(a.config.ID, false); err != nil {
			return fmt.Errorf("stop RabbitMQ consumer: %w", err)
		}
	case err := <-heartbeatErrors:
		if err != nil {
			cancel()
			if cancelErr := channel.Cancel(a.config.ID, false); cancelErr != nil {
				return fmt.Errorf("%v; stop RabbitMQ consumer: %w", err, cancelErr)
			}
			workers.Wait()
			return err
		}
	case err := <-errorsFromWorkers:
		cancel()
		if cancelErr := channel.Cancel(a.config.ID, false); cancelErr != nil {
			return fmt.Errorf("%v; stop RabbitMQ consumer: %w", err, cancelErr)
		}
		workers.Wait()
		return err
	case <-workersDone:
		if ctx.Err() != nil {
			return nil
		}
		return errors.New("RabbitMQ job consumer stopped unexpectedly")
	}
	workers.Wait()
	return nil
}

func (a *Agent) heartbeatLoop(ctx context.Context, errs chan<- error) {
	ticker := time.NewTicker(a.config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			body := map[string]any{"active_jobs": a.activeLeases()}
			if err := a.request(ctx, http.MethodPost, "/v1/workers/"+a.config.ID+"/heartbeat", body, nil); err != nil {
				select {
				case errs <- fmt.Errorf("send worker heartbeat: %w", err):
				default:
				}
				return
			}
		}
	}
}

func (a *Agent) execute(parent context.Context, delivery amqp.Delivery) error {
	var job Job
	if err := json.Unmarshal(delivery.Body, &job); err != nil {
		if nackErr := delivery.Nack(false, false); nackErr != nil {
			return fmt.Errorf("decode job message: %v; reject message: %w", err, nackErr)
		}
		return fmt.Errorf("decode job message: %w", err)
	}
	if job.ID == "" || strings.TrimSpace(job.Command) == "" {
		if err := delivery.Nack(false, false); err != nil {
			return fmt.Errorf("reject invalid job: %w", err)
		}
		return errors.New("job message missing id or command")
	}

	started := map[string]any{
		"status": "running", "worker_id": a.config.ID, "execution_id": job.ExecutionID,
	}
	var claim Job
	if err := a.request(parent, http.MethodPost, "/v1/jobs/"+job.ID+"/status", started, &claim); err != nil {
		if errors.Is(err, ErrStaleExecution) {
			if ackErr := delivery.Ack(false); ackErr != nil {
				return fmt.Errorf("acknowledge stale job %s: %w", job.ID, ackErr)
			}
			return nil
		}
		if nackErr := delivery.Nack(false, true); nackErr != nil {
			return fmt.Errorf("report job start: %v; requeue message: %w", err, nackErr)
		}
		return fmt.Errorf("report job start: %w", err)
	}
	if !claim.Claimed {
		if err := delivery.Ack(false); err != nil {
			return fmt.Errorf("acknowledge already-claimed job %s: %w", job.ID, err)
		}
		return nil
	}
	job.ExecutionID = claim.ExecutionID
	a.setActive(job.ID, job.ExecutionID)
	defer a.removeActive(job.ID)

	jobCtx, cancel := context.WithTimeout(parent, a.config.JobTimeout)
	defer cancel()
	output, executionErr := executor.Run(jobCtx, job.Command)
	status := "succeeded"
	report := map[string]any{
		"status": status, "worker_id": a.config.ID, "execution_id": job.ExecutionID, "result": output,
	}
	if executionErr != nil {
		status = "failed"
		report["status"] = status
		report["result"] = output
		report["error"] = executionErr.Error()
	}
	if err := a.request(parent, http.MethodPost, "/v1/jobs/"+job.ID+"/status", report, nil); err != nil {
		if errors.Is(err, ErrStaleExecution) {
			if ackErr := delivery.Ack(false); ackErr != nil {
				return fmt.Errorf("acknowledge stale result for job %s: %w", job.ID, ackErr)
			}
			return nil
		}
		if nackErr := delivery.Nack(false, true); nackErr != nil {
			return fmt.Errorf("report job %s: %v; requeue message: %w", status, err, nackErr)
		}
		return fmt.Errorf("report job %s: %w", status, err)
	}
	if err := delivery.Ack(false); err != nil {
		return fmt.Errorf("acknowledge job %s: %w", job.ID, err)
	}
	if executionErr != nil {
		// A non-zero exit or timeout is an expected job outcome, not a worker failure.
		return nil
	}
	return nil
}

func (a *Agent) request(ctx context.Context, method, path string, body any, result any) error {
	var encoded io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		encoded = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(a.config.ControlPlaneURL, "/")+path, encoded)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("request control plane: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		bodyBytes, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
		if readErr != nil {
			return fmt.Errorf("control plane returned %s (read response: %w)", response.Status, readErr)
		}
		message := strings.TrimSpace(string(bodyBytes))
		if response.StatusCode == http.StatusConflict {
			return fmt.Errorf("%w: %s", ErrStaleExecution, message)
		}
		return fmt.Errorf("control plane returned %s: %s", response.Status, message)
	}
	if result != nil {
		if err := json.NewDecoder(response.Body).Decode(result); err != nil {
			return fmt.Errorf("decode control-plane response: %w", err)
		}
	}
	return nil
}

var ErrStaleExecution = errors.New("stale execution rejected by control plane")

func (a *Agent) setActive(jobID string, executionID int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.active[jobID] = executionID
}

func (a *Agent) removeActive(jobID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.active, jobID)
}

func (a *Agent) activeLeases() []ActiveLease {
	a.mu.RLock()
	defer a.mu.RUnlock()
	leases := make([]ActiveLease, 0, len(a.active))
	for jobID, executionID := range a.active {
		leases = append(leases, ActiveLease{JobID: jobID, ExecutionID: executionID})
	}
	return leases
}

func Hostname() string {
	name, err := os.Hostname()
	if err == nil && name != "" {
		return name
	}
	return "worker"
}
