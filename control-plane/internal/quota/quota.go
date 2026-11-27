package quota

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

var (
	ErrExceeded       = errors.New("tenant quota exceeded")
	ErrTenantNotFound = errors.New("tenant not found")
)

type Request struct {
	CPU      int
	MemoryMB int64
	GPU      int
}

type Limits struct {
	CPU            int
	MemoryMB       int64
	GPU            int
	ConcurrentJobs int
}

type Usage struct {
	CPU            int
	MemoryMB       int64
	GPU            int
	ConcurrentJobs int
}

func Check(limits Limits, usage Usage, request Request) error {
	if usage.CPU > limits.CPU || request.CPU > limits.CPU-usage.CPU {
		return fmt.Errorf("%w: CPU usage %d + request %d exceeds limit %d", ErrExceeded, usage.CPU, request.CPU, limits.CPU)
	}
	if usage.MemoryMB > limits.MemoryMB || request.MemoryMB > limits.MemoryMB-usage.MemoryMB {
		return fmt.Errorf("%w: memory usage %d MiB + request %d MiB exceeds limit %d MiB",
			ErrExceeded, usage.MemoryMB, request.MemoryMB, limits.MemoryMB)
	}
	if usage.GPU > limits.GPU || request.GPU > limits.GPU-usage.GPU {
		return fmt.Errorf("%w: GPU usage %d + request %d exceeds limit %d", ErrExceeded, usage.GPU, request.GPU, limits.GPU)
	}
	if usage.ConcurrentJobs >= limits.ConcurrentJobs {
		return fmt.Errorf("%w: active jobs %d + request 1 exceeds limit %d",
			ErrExceeded, usage.ConcurrentJobs, limits.ConcurrentJobs)
	}
	return nil
}

func CheckTransaction(ctx context.Context, tx *sql.Tx, tenantID string, request Request) error {
	limits, err := LockTenant(ctx, tx, tenantID)
	if err != nil {
		return err
	}
	return CheckTransactionWithLimits(ctx, tx, tenantID, limits, request)
}

func LockTenant(ctx context.Context, tx *sql.Tx, tenantID string) (Limits, error) {
	var limits Limits
	err := tx.QueryRowContext(ctx, `SELECT max_cpu, max_ram_mb, max_gpu, max_concurrent_jobs
		FROM tenants WHERE id = ? FOR UPDATE`, tenantID).Scan(
		&limits.CPU, &limits.MemoryMB, &limits.GPU, &limits.ConcurrentJobs)
	if errors.Is(err, sql.ErrNoRows) {
		return Limits{}, fmt.Errorf("%w: %q", ErrTenantNotFound, tenantID)
	}
	if err != nil {
		return Limits{}, fmt.Errorf("load tenant quota: %w", err)
	}
	return limits, nil
}

func CheckTransactionWithLimits(
	ctx context.Context,
	tx *sql.Tx,
	tenantID string,
	limits Limits,
	request Request,
) error {
	var usage Usage
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(req_cpu), 0),
		COALESCE(SUM(req_memory_mb), 0), COALESCE(SUM(req_gpu), 0), COUNT(*)
		FROM jobs_projection
		WHERE tenant_id = ? AND status IN ('scheduled', 'leased', 'running')`,
		tenantID).Scan(&usage.CPU, &usage.MemoryMB, &usage.GPU, &usage.ConcurrentJobs)
	if err != nil {
		return fmt.Errorf("load active tenant job usage: %w", err)
	}
	return Check(limits, usage, request)
}
