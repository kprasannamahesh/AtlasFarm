package executor

import (
	"context"
	"strings"
	"testing"
)

func TestRunCapturesCommandOutput(t *testing.T) {
	output, err := Run(context.Background(), "echo atlasfarm-executor-test")
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !strings.Contains(output, "atlasfarm-executor-test") {
		t.Fatalf("Run() output = %q, want command output", output)
	}
}

func TestRunReturnsCommandFailure(t *testing.T) {
	_, err := Run(context.Background(), "exit 7")
	if err == nil {
		t.Fatal("Run() error = nil, want nonzero exit failure")
	}
}
