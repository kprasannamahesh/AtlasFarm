package rollout

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestEvaluateAutomaticallyRollsBackFailingCanary(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	now := time.Now()
	mock.ExpectQuery("SELECT id, target_version, canary_percentage").
		WillReturnRows(sqlmock.NewRows([]string{"id", "target_version", "canary_percentage",
			"error_threshold_pct", "status", "created_at", "updated_at"}).
			AddRow(3, "v2.0.0-canary", 25, 10.0, "ACTIVE", now, now))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\), COALESCE").
		WithArgs("v2.0.0-canary", now).
		WillReturnRows(sqlmock.NewRows([]string{"completed", "failures"}).AddRow(5, 2))
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id FROM rollouts").
		WithArgs(int64(3)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(3))
	mock.ExpectExec("UPDATE rollouts SET canary_percentage = 0").
		WithArgs(int64(3)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT id, target_version, canary_percentage").
		WithArgs(int64(3)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "target_version", "canary_percentage",
			"error_threshold_pct", "status", "created_at", "updated_at"}).
			AddRow(3, "v2.0.0-canary", 0, 10.0, "ROLLED_BACK", now, now))
	mock.ExpectCommit()

	rolledBack, err := store.Evaluate(context.Background())
	if err != nil || !rolledBack {
		t.Fatalf("Evaluate() = (%v, %v), want automatic rollback", rolledBack, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEvaluateRequiresFiveCompletedCanaryJobs(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	now := time.Now()
	mock.ExpectQuery("SELECT id, target_version, canary_percentage").
		WillReturnRows(sqlmock.NewRows([]string{"id", "target_version", "canary_percentage",
			"error_threshold_pct", "status", "created_at", "updated_at"}).
			AddRow(3, "v2.0.0-canary", 25, 10.0, "ACTIVE", now, now))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\), COALESCE").
		WithArgs("v2.0.0-canary", now).
		WillReturnRows(sqlmock.NewRows([]string{"completed", "failures"}).AddRow(4, 4))

	rolledBack, err := store.Evaluate(context.Background())
	if err != nil || rolledBack {
		t.Fatalf("Evaluate() = (%v, %v), want no rollback before five outcomes", rolledBack, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEvaluateCompletesHealthyFullRollout(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	now := time.Now()
	mock.ExpectQuery("SELECT id, target_version, canary_percentage").
		WillReturnRows(sqlmock.NewRows([]string{"id", "target_version", "canary_percentage",
			"error_threshold_pct", "status", "created_at", "updated_at"}).
			AddRow(4, "v2.0.0", 100, 10.0, "ACTIVE", now, now))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\), COALESCE").
		WithArgs("v2.0.0", now).
		WillReturnRows(sqlmock.NewRows([]string{"completed", "failures"}).AddRow(5, 0))
	mock.ExpectExec("UPDATE rollouts SET status = 'COMPLETED'").
		WithArgs(int64(4)).WillReturnResult(sqlmock.NewResult(0, 1))

	rolledBack, err := store.Evaluate(context.Background())
	if err != nil || rolledBack {
		t.Fatalf("Evaluate() = (%v, %v), want healthy completion without rollback", rolledBack, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
