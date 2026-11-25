package quota

import (
	"errors"
	"testing"
)

func TestCheckQuotaLimits(t *testing.T) {
	limits := Limits{CPU: 8, MemoryMB: 8192, GPU: 2, ConcurrentJobs: 4}
	tests := []struct {
		name    string
		usage   Usage
		request Request
		wantErr bool
	}{
		{
			name:    "within all quotas",
			usage:   Usage{CPU: 2, MemoryMB: 1024, GPU: 1, ConcurrentJobs: 1},
			request: Request{CPU: 2, MemoryMB: 2048, GPU: 1},
		},
		{
			name:    "cpu limit breach",
			usage:   Usage{CPU: 7},
			request: Request{CPU: 2, MemoryMB: 1},
			wantErr: true,
		},
		{
			name:    "memory limit breach",
			usage:   Usage{MemoryMB: 8000},
			request: Request{CPU: 1, MemoryMB: 193},
			wantErr: true,
		},
		{
			name:    "gpu limit breach",
			usage:   Usage{GPU: 2},
			request: Request{CPU: 1, MemoryMB: 1, GPU: 1},
			wantErr: true,
		},
		{
			name:    "concurrent job limit breach",
			usage:   Usage{ConcurrentJobs: 4},
			request: Request{CPU: 1, MemoryMB: 1},
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := Check(limits, test.usage, test.request)
			if test.wantErr != errors.Is(err, ErrExceeded) {
				t.Fatalf("Check() error = %v, wantErr=%t", err, test.wantErr)
			}
		})
	}
}
