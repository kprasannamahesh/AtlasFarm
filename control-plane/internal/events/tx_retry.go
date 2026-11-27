package events

import (
	"context"
	"database/sql"
	"errors"
	"math/rand"
	"strings"
	"time"

	mysql "github.com/go-sql-driver/mysql"
)

const deadlockMaxRetries = 5

type jobOperationResult struct {
	job     Job
	changed bool
}

func withTxRetry[T any](ctx context.Context, db *sql.DB, maxRetries int, fn func() (T, error)) (T, error) {
	var zero T
	if db == nil {
		return zero, errors.New("retry transaction: database is nil")
	}
	if fn == nil {
		return zero, errors.New("retry transaction: callback is nil")
	}

	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		result, err := fn()
		if err == nil {
			return result, nil
		}
		if attempt >= maxRetries || !isMySQLDeadlock(err) {
			return zero, err
		}

		delay := time.Duration(2+rand.Intn(14)) * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return zero, ctx.Err()
		}
	}
}

func isMySQLDeadlock(err error) bool {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1213 {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "error 1213")
}
