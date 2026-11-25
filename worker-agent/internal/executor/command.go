package executor

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sync"
)

const outputLimit = 1 << 20

type limitedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	limit  int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(p)
	remaining := b.limit - b.buffer.Len()
	if remaining > 0 {
		if remaining > len(p) {
			remaining = len(p)
		}
		_, _ = b.buffer.Write(p[:remaining])
	}
	return n, nil
}

func (b *limitedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func Run(ctx context.Context, command string) (string, error) {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		shell := os.Getenv("COMSPEC")
		if shell == "" {
			shell = "cmd.exe"
		}
		cmd = exec.CommandContext(ctx, shell, "/C", command)
	} else {
		cmd = exec.CommandContext(ctx, "sh", "-c", command)
	}

	output := &limitedBuffer{limit: outputLimit}
	cmd.Stdout = output
	cmd.Stderr = output
	err := cmd.Run()
	result := output.String()
	if ctx.Err() != nil {
		return result, fmt.Errorf("job timed out or was cancelled: %w", ctx.Err())
	}
	if err != nil {
		return result, fmt.Errorf("command failed: %w", err)
	}
	return result, nil
}
