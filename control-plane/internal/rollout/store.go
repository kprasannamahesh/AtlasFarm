package rollout

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"
)

var (
	ErrNotFound       = errors.New("rollout not found")
	ErrInvalidRequest = errors.New("invalid rollout request")
)

type Rollout struct {
	ID                int64     `json:"id"`
	TargetVersion     string    `json:"target_version"`
	CanaryPercentage  int       `json:"canary_percentage"`
	ErrorThresholdPct float64   `json:"error_threshold_pct"`
	Status            string    `json:"status"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type CreateRequest struct {
	TargetVersion     string
	CanaryPercentage  int
	ErrorThresholdPct float64
}

type Store struct {
	db *sql.DB
}

func New(db *sql.DB) *Store {
	return &Store{db: db}
}

func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS rollouts (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		target_version VARCHAR(64) NOT NULL,
		canary_percentage INT NOT NULL,
		error_threshold_pct DECIMAL(5,2) NOT NULL,
		status ENUM('ACTIVE', 'ROLLED_BACK', 'COMPLETED') NOT NULL,
		created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
		updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
			ON UPDATE CURRENT_TIMESTAMP(6),
		KEY idx_rollouts_status_created (status, created_at)
	)`)
	if err != nil {
		return fmt.Errorf("create rollouts table: %w", err)
	}
	return nil
}

func (s *Store) GetCurrent(ctx context.Context) (Rollout, error) {
	rollout, err := scanRollout(s.db.QueryRowContext(ctx, `SELECT id, target_version, canary_percentage,
		error_threshold_pct, status, created_at, updated_at FROM rollouts
		ORDER BY id DESC LIMIT 1`))
	if errors.Is(err, sql.ErrNoRows) {
		return Rollout{}, ErrNotFound
	}
	if err != nil {
		return Rollout{}, fmt.Errorf("get current rollout: %w", err)
	}
	return rollout, nil
}

func (s *Store) Create(ctx context.Context, req CreateRequest) (Rollout, error) {
	if req.TargetVersion == "" || len(req.TargetVersion) > 64 ||
		req.CanaryPercentage < 0 || req.CanaryPercentage > 100 ||
		req.ErrorThresholdPct < 0 || req.ErrorThresholdPct > 100 {
		return Rollout{}, fmt.Errorf("%w: target_version, canary_percentage 0-100, error_threshold_pct 0-100 are required", ErrInvalidRequest)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Rollout{}, fmt.Errorf("begin rollout creation: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE rollouts SET status = 'COMPLETED'
		WHERE status = 'ACTIVE'`); err != nil {
		return Rollout{}, fmt.Errorf("complete previous active rollouts: %w", err)
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO rollouts
		(target_version, canary_percentage, error_threshold_pct, status)
		VALUES (?, ?, ?, 'ACTIVE')`,
		req.TargetVersion, req.CanaryPercentage, req.ErrorThresholdPct)
	if err != nil {
		return Rollout{}, fmt.Errorf("create rollout: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return Rollout{}, fmt.Errorf("read new rollout ID: %w", err)
	}
	rollout, err := scanRollout(tx.QueryRowContext(ctx, `SELECT id, target_version, canary_percentage,
		error_threshold_pct, status, created_at, updated_at FROM rollouts WHERE id = ?`, id))
	if err != nil {
		return Rollout{}, fmt.Errorf("read created rollout: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Rollout{}, fmt.Errorf("commit rollout creation: %w", err)
	}
	return rollout, nil
}

func (s *Store) Rollback(ctx context.Context) (Rollout, error) {
	return s.rollback(ctx, "manual")
}

func (s *Store) rollback(ctx context.Context, reason string) (Rollout, error) {
	return s.rollbackID(ctx, 0, reason)
}

func (s *Store) rollbackID(ctx context.Context, rolloutID int64, reason string) (Rollout, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Rollout{}, fmt.Errorf("begin rollout rollback: %w", err)
	}
	defer tx.Rollback()
	var id int64
	if rolloutID == 0 {
		err = tx.QueryRowContext(ctx, `SELECT id FROM rollouts WHERE status = 'ACTIVE'
			ORDER BY id DESC LIMIT 1 FOR UPDATE`).Scan(&id)
	} else {
		err = tx.QueryRowContext(ctx, `SELECT id FROM rollouts
			WHERE id = ? AND status = 'ACTIVE' FOR UPDATE`, rolloutID).Scan(&id)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return Rollout{}, ErrNotFound
	}
	if err != nil {
		return Rollout{}, fmt.Errorf("lock active rollout: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE rollouts SET canary_percentage = 0,
		status = 'ROLLED_BACK' WHERE id = ?`, id); err != nil {
		return Rollout{}, fmt.Errorf("mark rollout rolled back: %w", err)
	}
	rollout, err := scanRollout(tx.QueryRowContext(ctx, `SELECT id, target_version, canary_percentage,
		error_threshold_pct, status, created_at, updated_at FROM rollouts WHERE id = ?`, id))
	if err != nil {
		return Rollout{}, fmt.Errorf("read rolled-back rollout: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Rollout{}, fmt.Errorf("commit rollout rollback: %w", err)
	}
	log.Printf("rollout rollback event: id=%d target_version=%s reason=%s", rollout.ID, rollout.TargetVersion, reason)
	return rollout, nil
}

func (s *Store) Evaluate(ctx context.Context) (bool, error) {
	current, err := s.GetCurrent(ctx)
	if errors.Is(err, ErrNotFound) || (err == nil && current.Status != "ACTIVE") {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var completed, failures int
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(j.status = 'failed'), 0)
		FROM jobs_projection j JOIN workers w ON w.id = j.worker_id
		WHERE w.runner_version = ? AND j.status IN ('succeeded', 'failed')
		AND j.updated_at >= ?`, current.TargetVersion, current.CreatedAt).Scan(&completed, &failures)
	if err != nil {
		return false, fmt.Errorf("measure canary outcomes: %w", err)
	}
	if completed < 5 {
		return false, nil
	}
	errorPercentage := float64(failures) * 100 / float64(completed)
	if errorPercentage > current.ErrorThresholdPct {
		rolledBack, err := s.rollbackID(ctx, current.ID, "error_threshold_exceeded")
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		log.Printf("rollout rollback details: id=%d target_version=%s completed=%d failures=%d error_pct=%.2f threshold_pct=%.2f",
			rolledBack.ID, current.TargetVersion, completed, failures, errorPercentage, current.ErrorThresholdPct)
		return true, nil
	}
	if current.CanaryPercentage == 100 {
		if err := s.complete(ctx, current.ID); err != nil && !errors.Is(err, ErrNotFound) {
			return false, err
		}
	}
	return false, nil
}

func (s *Store) complete(ctx context.Context, id int64) error {
	result, err := s.db.ExecContext(ctx, `UPDATE rollouts SET status = 'COMPLETED'
		WHERE id = ? AND status = 'ACTIVE' AND canary_percentage = 100`, id)
	if err != nil {
		return fmt.Errorf("complete rollout: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read rollout completion result: %w", err)
	}
	if changed == 0 {
		return ErrNotFound
	}
	log.Printf("rollout completion event: id=%d", id)
	return nil
}

func scanRollout(row interface{ Scan(...any) error }) (Rollout, error) {
	var rollout Rollout
	err := row.Scan(&rollout.ID, &rollout.TargetVersion, &rollout.CanaryPercentage,
		&rollout.ErrorThresholdPct, &rollout.Status, &rollout.CreatedAt, &rollout.UpdatedAt)
	return rollout, err
}
