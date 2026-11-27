package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kprasannamahesh/AtlasFarm/control-plane/internal/events"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

type RabbitMQ struct {
	url      string
	channel  *amqp.Channel
	conn     *amqp.Connection
	queue    string
	exchange string
	confirms <-chan amqp.Confirmation
	mu       sync.Mutex
	active   atomic.Pointer[activeConnection]
	closed   bool
}

type activeConnection struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func Connect(url, queue string) (*RabbitMQ, *amqp.Connection, error) {
	conn, channel, confirms, err := open(url, queue)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	publisher := &RabbitMQ{
		url: url, conn: conn, channel: channel, confirms: confirms,
		queue: queue, exchange: "atlasfarm.jobs",
	}
	publisher.activateConnection(conn, channel)
	return publisher, conn, nil
}

func open(url, queue string) (*amqp.Connection, *amqp.Channel, <-chan amqp.Confirmation, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, nil, err
	}
	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("open RabbitMQ channel: %w", err)
	}
	exchange := "atlasfarm.jobs"
	if err := channel.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("declare job exchange: %w", err)
	}
	if _, err := channel.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("declare job queue: %w", err)
	}
	if err := channel.QueueBind(queue, queue, exchange, false, nil); err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("bind job queue: %w", err)
	}
	if err := channel.Confirm(false); err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("enable RabbitMQ publisher confirmations: %w", err)
	}
	confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	return conn, channel, confirms, nil
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
	if err := ctx.Err(); err != nil {
		return err
	}
	body, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("encode job message: %w", err)
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	headers := make(amqp.Table, len(carrier))
	for key, value := range carrier {
		headers[key] = value
	}
	for {
		if err := r.connectWithBackoff(ctx); err != nil {
			return err
		}
		err = r.channel.PublishWithContext(ctx, r.exchange, routingKey, false, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now().UTC(),
			MessageId:    job.ID,
			Headers:      headers,
			Body:         body,
		})
		if err != nil {
			r.disconnect()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		timer := time.NewTimer(10 * time.Second)
		select {
		case confirmation, ok := <-r.confirms:
			timer.Stop()
			if ok && confirmation.Ack {
				return nil
			}
			if ok {
				return fmt.Errorf("RabbitMQ rejected job %s", job.ID)
			}
			r.disconnect()
		case <-timer.C:
			r.disconnect()
		case <-ctx.Done():
			timer.Stop()
			r.disconnect()
			return ctx.Err()
		}
	}
}

func (r *RabbitMQ) connectWithBackoff(ctx context.Context) error {
	delay := 500 * time.Millisecond
	for {
		if err := r.connectOnce(); err == nil {
			return nil
		} else if errors.Is(err, errPublisherClosed) {
			return err
		} else {
			log.Printf("RabbitMQ reconnect failed: %v; retrying in %s", err, delay)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
		if delay < 8*time.Second {
			delay *= 2
		}
	}
}

var errPublisherClosed = errors.New("RabbitMQ publisher is closed")

func (r *RabbitMQ) connectOnce() error {
	if r.closed {
		return errPublisherClosed
	}
	active := r.active.Load()
	if active != nil && active.conn == r.conn && active.channel == r.channel &&
		!r.conn.IsClosed() && !r.channel.IsClosed() {
		return nil
	}
	r.disconnect()
	conn, channel, confirms, err := open(r.url, r.queue)
	if err != nil {
		return err
	}
	r.conn, r.channel, r.confirms = conn, channel, confirms
	r.activateConnection(conn, channel)
	return nil
}

func (r *RabbitMQ) activateConnection(conn *amqp.Connection, channel *amqp.Channel) {
	active := &activeConnection{conn: conn, channel: channel}
	r.active.Store(active)
	connectionClosed := conn.NotifyClose(make(chan *amqp.Error, 1))
	channelClosed := channel.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		select {
		case <-connectionClosed:
		case <-channelClosed:
		}
		r.active.CompareAndSwap(active, nil)
	}()
}

func (r *RabbitMQ) RunReconnectLoop(ctx context.Context) {
	delay := 500 * time.Millisecond
	for {
		if ctx.Err() != nil {
			return
		}
		if r.IsClosed() {
			r.mu.Lock()
			err := r.connectOnce()
			r.mu.Unlock()
			if err != nil && !errors.Is(err, errPublisherClosed) && ctx.Err() == nil {
				log.Printf("RabbitMQ reconnect manager failed: %v", err)
				if delay < 8*time.Second {
					delay *= 2
				}
			} else {
				delay = 500 * time.Millisecond
			}
		} else {
			delay = 500 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (r *RabbitMQ) disconnect() {
	r.active.Store(nil)
	if r.channel != nil {
		_ = r.channel.Close()
		r.channel = nil
	}
	if r.conn != nil {
		_ = r.conn.Close()
		r.conn = nil
	}
	r.confirms = nil
}

func (r *RabbitMQ) IsClosed() bool {
	active := r.active.Load()
	return active == nil || active.conn == nil || active.channel == nil ||
		active.conn.IsClosed() || active.channel.IsClosed()
}

func (r *RabbitMQ) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.active.Store(nil)
	var closeErr error
	if r.channel != nil {
		closeErr = r.channel.Close()
	}
	if r.conn != nil {
		if err := r.conn.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}
