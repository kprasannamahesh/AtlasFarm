package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/events"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	channel  *amqp.Channel
	queue    string
	exchange string
	confirms <-chan amqp.Confirmation
	mu       sync.Mutex
	failure  error
}

func Connect(url, queue string) (*RabbitMQ, *amqp.Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("open RabbitMQ channel: %w", err)
	}
	exchange := "atlasfarm.jobs"
	if err := channel.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return nil, nil, fmt.Errorf("declare job exchange: %w", err)
	}
	if _, err := channel.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return nil, nil, fmt.Errorf("declare job queue: %w", err)
	}
	if err := channel.QueueBind(queue, queue, exchange, false, nil); err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return nil, nil, fmt.Errorf("bind job queue: %w", err)
	}
	if err := channel.Confirm(false); err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return nil, nil, fmt.Errorf("enable RabbitMQ publisher confirmations: %w", err)
	}
	confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	return &RabbitMQ{channel: channel, queue: queue, exchange: exchange, confirms: confirms}, conn, nil
}

func (r *RabbitMQ) Publish(ctx context.Context, job events.Job) error {
	return r.publish(ctx, r.queue, job)
}

func (r *RabbitMQ) PublishToWorker(ctx context.Context, workerID string, job events.Job) error {
	if workerID == "" {
		return errors.New("worker ID is required for targeted publish")
	}
	return r.publish(ctx, "jobs.worker."+workerID, job)
}

func (r *RabbitMQ) publish(ctx context.Context, routingKey string, job events.Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failure != nil {
		return fmt.Errorf("RabbitMQ publisher unavailable: %w", r.failure)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	body, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("encode job message: %w", err)
	}
	if err := r.channel.PublishWithContext(ctx, r.exchange, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now().UTC(),
		MessageId:    job.ID,
		Body:         body,
	}); err != nil {
		r.failure = err
		return fmt.Errorf("publish with routing key %q: %w", routingKey, err)
	}
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case confirmation, ok := <-r.confirms:
		if !ok {
			r.failure = errors.New("publisher confirmation channel closed")
			return r.failure
		}
		if !confirmation.Ack {
			return fmt.Errorf("RabbitMQ rejected job %s", job.ID)
		}
		return nil
	case <-timer.C:
		r.failure = fmt.Errorf("confirmation timed out for job %s", job.ID)
		return r.failure
	case <-ctx.Done():
		// Consume the ordered broker confirmation before releasing the publisher lock.
		select {
		case confirmation, ok := <-r.confirms:
			if !ok {
				r.failure = errors.New("publisher confirmation channel closed")
				return r.failure
			}
			if !confirmation.Ack {
				return fmt.Errorf("RabbitMQ rejected job %s", job.ID)
			}
			return ctx.Err()
		case <-timer.C:
			r.failure = fmt.Errorf("confirmation timed out after request cancellation: %w", ctx.Err())
			return fmt.Errorf("timed out waiting for RabbitMQ confirmation for job %s: %w", job.ID, r.failure)
		}
	}
}

func (r *RabbitMQ) IsClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failure != nil || r.channel.IsClosed()
}

func (r *RabbitMQ) Close() error {
	return r.channel.Close()
}
