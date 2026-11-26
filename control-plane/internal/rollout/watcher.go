package rollout

import (
	"context"
	"errors"
	"log"
	"time"
)

const WatchInterval = 10 * time.Second

type Watcher struct {
	store *Store
}

func NewWatcher(store *Store) *Watcher {
	return &Watcher{store: store}
}

func (w *Watcher) Run(ctx context.Context) {
	ticker := time.NewTicker(WatchInterval)
	defer ticker.Stop()
	for {
		if _, err := w.store.Evaluate(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("evaluate canary rollout: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
