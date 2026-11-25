package main

import (
	"net"
	"testing"
)

func TestListenMetricsFallsBackWhenRequestedPortIsBusy(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()

	listener, err := listenMetrics(occupied.Addr().String())
	if err != nil {
		t.Fatalf("listenMetrics() error = %v", err)
	}
	defer listener.Close()

	if listener.Addr().String() == occupied.Addr().String() {
		t.Fatalf("fallback listener reused occupied address %s", listener.Addr())
	}
}
