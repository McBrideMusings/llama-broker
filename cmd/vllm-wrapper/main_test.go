package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestWakeUpVLLM(t *testing.T) {
	// Test successful wake up
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/wake_up" {
			t.Errorf("Unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	if err := wakeUpVLLM(ts.URL); err != nil {
		t.Fatalf("wakeUpVLLM failed: %v", err)
	}

	// Test failure when server returns error
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/wake_up" {
			t.Errorf("Unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts2.Close()

	if err := wakeUpVLLM(ts2.URL); err == nil {
		t.Errorf("wakeUpVLLM expected error for non-200 response")
	}
}

func TestWaitForHealthy(t *testing.T) {
	// Test successful health check
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Errorf("Unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"data":[]}`))
	}))
	defer ts.Close()

	if err := waitForHealthyWithPath(ts.URL, "/v1/models", 2*time.Second); err != nil {
		t.Fatalf("waitForHealthy failed: %v", err)
	}

	// Test timeout: server delays response longer than context timeout
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Delay 2 seconds
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"data":[]}`))
	}))
	defer ts2.Close()

	err := waitForHealthyWithPath(ts2.URL, "/v1/models", 1*time.Second)
	if err == nil {
		t.Errorf("waitForHealthy expected timeout error")
		return
	}
	if err != context.DeadlineExceeded {
		t.Errorf("waitForHealthy expected context deadline exceeded, got %v", err)
	}
}

func TestSleepCommandMarshal(t *testing.T) {
	// We test the sleep command by checking the JSON marshaling we use in sleepCmd.
	// Since sleepCmd is not easily unit-testable without exposing more, we test the structure.
	body := map[string]int{"level": 1}
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}
	expected := `{"level":1}`
	if string(data) != expected {
		t.Errorf("Expected %s, got %s", expected, string(data))
	}
}

// TestStartDaemon tests that startDaemon returns an error when the start command exits
// quickly and the daemon does not become healthy.
func TestStartDaemon(t *testing.T) {
	// Use a start command that exits immediately (true) and a health URL that will not respond.
	err := startDaemon([]string{"true"}, "http://127.0.0.1:12345/health", "/health", 10*time.Millisecond)
	if err == nil {
		t.Fatalf("startDaemon expected error but got nil")
	}
	if !strings.Contains(err.Error(), "daemon did not become healthy") {
		t.Errorf("error expected to contain 'daemon did not become healthy', got %v", err)
	}
}

// TestStartDaemon_ReapsKilledProcess verifies that a daemon killed for never
// becoming healthy is reaped rather than left as a zombie.
func TestStartDaemon_ReapsKilledProcess(t *testing.T) {
	// startDaemon logs the PID it started; read it from there.
	var logs bytes.Buffer
	log.SetOutput(&logs)
	defer log.SetOutput(os.Stderr)

	if err := startDaemon([]string{"sleep", "30"}, "http://127.0.0.1:12345/health", "/health", 10*time.Millisecond); err == nil {
		t.Fatal("expected error (health check fails)")
	}

	m := regexp.MustCompile(`Started daemon with PID (\d+)`).FindStringSubmatch(logs.String())
	if m == nil {
		t.Fatalf("no PID in startDaemon log: %q", logs.String())
	}
	pid, err := strconv.Atoi(m[1])
	if err != nil {
		t.Fatalf("parse pid %q: %v", m[1], err)
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		t.Fatalf("FindProcess: %v", err)
	}
	// Signal 0 succeeds for a live or zombie process and fails once reaped.
	if err := proc.Signal(syscall.Signal(0)); err == nil {
		t.Errorf("process %d still exists after startDaemon returned", pid)
	}
}

// TestStartDaemonArgv verifies that multiple startArgs are passed as separate argv values.
func TestStartDaemonArgv(t *testing.T) {
	tmpDir := t.TempDir()
	argvFile := filepath.Join(tmpDir, "argv.txt")

	// Write to a temp file and rename so argv.txt appears complete or not at all.
	script := filepath.Join(tmpDir, "write-argv.sh")
	if err := os.WriteFile(script, []byte(
		"#!/bin/bash\nprintf '%s\n' \"$@\" > \""+argvFile+".tmp\"\nmv \""+argvFile+".tmp\" \""+argvFile+"\"\nexit 0\n",
	), 0755); err != nil {
		t.Fatalf("write helper script: %v", err)
	}

	// Report healthy once the script has written argv.txt, so startDaemon
	// waits for the script instead of killing it at a fixed timeout.
	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := os.Stat(argvFile); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer health.Close()

	if err := startDaemon([]string{script, "arg1", "arg2", "arg3"}, health.URL, "/health", 30*time.Second); err != nil {
		t.Fatalf("startDaemon: %v", err)
	}

	content, err := os.ReadFile(argvFile)
	if err != nil {
		t.Fatalf("read argv file: %v", err)
	}
	got := strings.Split(strings.TrimSpace(string(content)), "\n")
	want := []string{"arg1", "arg2", "arg3"}
	if len(got) != len(want) {
		t.Fatalf("argv length: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("argv[%d]: got %q, want %q", i, got[i], want[i])
		}
	}
}
