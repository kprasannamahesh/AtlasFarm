package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPingDatabaseRetriesAfterTransientFailure(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectPing().WillReturnError(errors.New("connection reset"))
	mock.ExpectPing()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := pingDatabase(ctx, db); err != nil {
		t.Fatalf("pingDatabase() error = %v, want retry to succeed", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
