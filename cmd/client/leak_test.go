package main

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package when a test leaves goroutines running.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
