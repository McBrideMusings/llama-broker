package logmon

import (
	"bytes"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mostlygeek/llama-swap/internal/event"
)

func TestLogMonitor(t *testing.T) {
	logMonitor := NewWriter(io.Discard)

	var wg sync.WaitGroup

	client1Messages := make([]byte, 0)
	client2Messages := make([]byte, 0)

	defer logMonitor.Subscribe(nil, func(data []byte) {
		client1Messages = append(client1Messages, data...)
		wg.Done()
	})()

	defer logMonitor.Subscribe(nil, func(data []byte) {
		client2Messages = append(client2Messages, data...)
		wg.Done()
	})()

	wg.Add(6) // 2 x 3 writes

	logMonitor.Write([]byte("1"))
	logMonitor.Write([]byte("2"))
	logMonitor.Write([]byte("3"))

	wg.Wait()

	expectedHistory := "123"
	history := string(logMonitor.GetHistory())

	if history != expectedHistory {
		t.Errorf("Expected history: %s, got: %s", expectedHistory, history)
	}

	c1Data := string(client1Messages)
	if c1Data != expectedHistory {
		t.Errorf("Client1 expected %s, got: %s", expectedHistory, c1Data)
	}

	c2Data := string(client2Messages)
	if c2Data != expectedHistory {
		t.Errorf("Client2 expected %s, got: %s", expectedHistory, c2Data)
	}
}

func TestWrite_ImmutableBuffer(t *testing.T) {
	lm := NewWriter(io.Discard)

	msg := []byte("Hello, World!")
	lenmsg := len(msg)

	n, err := lm.Write(msg)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if n != lenmsg {
		t.Errorf("Expected %d bytes written but got %d", lenmsg, n)
	}

	msg[0] = 'B'

	history := lm.GetHistory()

	expected := []byte("Hello, World!")
	if !bytes.Equal(history, expected) {
		t.Errorf("Expected history to be %q, got %q", expected, history)
	}
}

func TestWrite_LogTimeFormat(t *testing.T) {
	lm := NewWriter(io.Discard)

	lm.timeFormat = time.RFC3339

	lm.Info("Hello, World!")

	history := lm.GetHistory()

	timestamp := ""
	fields := strings.Fields(string(history))
	if len(fields) > 0 {
		timestamp = fields[0]
	} else {
		t.Fatalf("Cannot extract string from history")
	}

	_, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		t.Fatalf("Cannot find timestamp: %v", err)
	}
}

func TestCircularBuffer_WrapAround(t *testing.T) {
	cb := newCircularBuffer(10)

	cb.Write([]byte("hello"))
	if got := string(cb.GetHistory()); got != "hello" {
		t.Errorf("Expected 'hello', got %q", got)
	}

	cb.Write([]byte("world"))
	if got := string(cb.GetHistory()); got != "helloworld" {
		t.Errorf("Expected 'helloworld', got %q", got)
	}

	cb.Write([]byte("12345"))
	if got := string(cb.GetHistory()); got != "world12345" {
		t.Errorf("Expected 'world12345', got %q", got)
	}

	cb.Write([]byte("abcdefghijklmnop"))
	if got := string(cb.GetHistory()); got != "ghijklmnop" {
		t.Errorf("Expected 'ghijklmnop', got %q", got)
	}
}

func TestCircularBuffer_BoundaryConditions(t *testing.T) {
	cb := newCircularBuffer(10)
	if got := cb.GetHistory(); got != nil {
		t.Errorf("Expected nil for empty buffer, got %q", got)
	}

	cb.Write([]byte("1234567890"))
	if got := string(cb.GetHistory()); got != "1234567890" {
		t.Errorf("Expected '1234567890', got %q", got)
	}

	cb = newCircularBuffer(10)
	cb.Write([]byte("12345"))
	cb.Write([]byte("67890"))
	if got := string(cb.GetHistory()); got != "1234567890" {
		t.Errorf("Expected '1234567890', got %q", got)
	}
}

func TestLogMonitor_LazyInit(t *testing.T) {
	lm := NewWriter(io.Discard)

	if lm.buffer != nil {
		t.Error("Expected buffer to be nil before first write")
	}

	if got := lm.GetHistory(); got != nil {
		t.Errorf("Expected nil history before first write, got %q", got)
	}

	lm.Write([]byte("test"))

	if lm.buffer == nil {
		t.Error("Expected buffer to be initialized after write")
	}

	if got := string(lm.GetHistory()); got != "test" {
		t.Errorf("Expected 'test', got %q", got)
	}
}

func TestLogMonitor_Clear(t *testing.T) {
	lm := NewWriter(io.Discard)

	lm.Write([]byte("hello"))
	if got := string(lm.GetHistory()); got != "hello" {
		t.Errorf("Expected 'hello', got %q", got)
	}

	lm.Clear()

	if lm.buffer != nil {
		t.Error("Expected buffer to be nil after Clear")
	}

	if got := lm.GetHistory(); got != nil {
		t.Errorf("Expected nil history after Clear, got %q", got)
	}
}

func TestLogMonitor_ClearAndReuse(t *testing.T) {
	lm := NewWriter(io.Discard)

	lm.Write([]byte("first"))
	lm.Clear()
	lm.Write([]byte("second"))

	if got := string(lm.GetHistory()); got != "second" {
		t.Errorf("Expected 'second' after clear and reuse, got %q", got)
	}
}

// TestLogMonitor_SubscribeDeliversEachLineOnce covers a line written before
// Subscribe whose broadcast arrives after it: the line belongs to history
// only, never to the live stream too.
func TestLogMonitor_SubscribeDeliversEachLineOnce(t *testing.T) {
	for _, tc := range []struct {
		withHistory bool
		want        string
	}{
		{withHistory: false, want: "LIVE\n"},
		{withHistory: true, want: "HIST\nLIVE\n"},
	} {
		// Build the monitor without its broadcast goroutine so HIST is still
		// waiting to be broadcast when Subscribe runs.
		lm := &Monitor{
			eventbus:    event.NewDispatcherConfig(1000),
			stdout:      io.Discard,
			broadcastCh: make(chan DataEvent, 1024),
			level:       LevelInfo,
		}
		lm.Write([]byte("HIST\n"))

		var mu sync.Mutex
		var got []byte
		record := func(data []byte) {
			mu.Lock()
			got = append(got, data...)
			mu.Unlock()
		}
		var history func([]byte)
		if tc.withHistory {
			history = record
		}
		live := make(chan struct{})
		cancel := lm.Subscribe(history, func(data []byte) {
			record(data)
			if string(data) == "LIVE\n" {
				close(live)
			}
		})

		go lm.broadcastLoop()
		lm.Write([]byte("LIVE\n"))

		select {
		case <-live:
		case <-time.After(5 * time.Second):
			t.Fatalf("withHistory=%v: live line never arrived", tc.withHistory)
		}
		cancel()
		close(lm.broadcastCh)

		mu.Lock()
		if string(got) != tc.want {
			t.Errorf("withHistory=%v: got %q, want %q", tc.withHistory, got, tc.want)
		}
		mu.Unlock()
	}
}

// TestLogMonitor_SubscribeDroppedNotice covers the dropped-bytes notice: a
// subscriber whose history holds the dropped line skips it, and one that
// subscribed before the drop receives it.
func TestLogMonitor_SubscribeDroppedNotice(t *testing.T) {
	// A one-slot hand-off with no broadcast goroutine yet: A fills the slot,
	// B is dropped.
	lm := &Monitor{
		eventbus:    event.NewDispatcherConfig(1000),
		stdout:      io.Discard,
		broadcastCh: make(chan DataEvent, 1),
		level:       LevelInfo,
	}

	type recorder struct {
		mu  sync.Mutex
		got []byte
		a   chan struct{}
		c   chan struct{}
	}
	subscribe := func() *recorder {
		r := &recorder{a: make(chan struct{}), c: make(chan struct{})}
		cancel := lm.Subscribe(nil, func(data []byte) {
			r.mu.Lock()
			r.got = append(r.got, data...)
			r.mu.Unlock()
			switch string(data) {
			case "A\n":
				close(r.a)
			case "C\n":
				close(r.c)
			}
		})
		t.Cleanup(cancel)
		return r
	}
	wait := func(ch chan struct{}, what string) {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s never arrived", what)
		}
	}

	before := subscribe()
	lm.Write([]byte("A\n"))
	lm.Write([]byte("B\n"))
	after := subscribe()

	go lm.broadcastLoop()
	wait(before.a, "A")
	lm.Write([]byte("C\n"))
	wait(before.c, "C at the early subscriber")
	wait(after.c, "C at the late subscriber")
	close(lm.broadcastCh)

	before.mu.Lock()
	if want := "\n— 2 bytes dropped —\nA\nC\n"; string(before.got) != want {
		t.Errorf("early subscriber: got %q, want %q", before.got, want)
	}
	before.mu.Unlock()
	after.mu.Lock()
	if want := "C\n"; string(after.got) != want {
		t.Errorf("late subscriber: got %q, want %q", after.got, want)
	}
	after.mu.Unlock()
}

// TestLogMonitor_DropsWhenSubscriberBlocked verifies that a stalled subscriber
// can never block Write (the upstream process's stdout drain) and that dropped
// data is reported in-stream with a marker once delivery resumes. See #875.
func TestLogMonitor_DropsWhenSubscriberBlocked(t *testing.T) {
	lm := NewWriter(io.Discard)

	release := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	var received [][]byte

	cancel := lm.Subscribe(nil, func(data []byte) {
		// Block the first delivery, stalling the broadcaster goroutine so the
		// hand-off channel and event queue fill and subsequent writes drop.
		once.Do(func() { <-release })
		mu.Lock()
		received = append(received, append([]byte(nil), data...))
		mu.Unlock()
	})
	defer cancel()

	// Flood well past the hand-off channel (1024) + event queue (1000)
	// capacity. None of these writes may block.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 4000; i++ {
			lm.Write([]byte("x"))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Write blocked while subscriber was stalled")
	}

	close(release)

	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		found := false
		for _, d := range received {
			if strings.Contains(string(d), "bytes dropped") {
				found = true
				break
			}
		}
		mu.Unlock()
		if found {
			return
		}
		select {
		case <-deadline:
			t.Fatal("expected a 'bytes dropped' marker after resuming delivery")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func BenchmarkLogMonitorWrite(b *testing.B) {
	smallMsg := []byte("small message\n")
	mediumMsg := []byte(strings.Repeat("medium message content ", 10) + "\n")
	largeMsg := []byte(strings.Repeat("large message content for benchmarking ", 100) + "\n")

	b.Run("SmallWrite", func(b *testing.B) {
		lm := NewWriter(io.Discard)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			lm.Write(smallMsg)
		}
	})

	b.Run("MediumWrite", func(b *testing.B) {
		lm := NewWriter(io.Discard)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			lm.Write(mediumMsg)
		}
	})

	b.Run("LargeWrite", func(b *testing.B) {
		lm := NewWriter(io.Discard)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			lm.Write(largeMsg)
		}
	})

	b.Run("WithSubscribers", func(b *testing.B) {
		lm := NewWriter(io.Discard)
		for i := 0; i < 5; i++ {
			lm.Subscribe(nil, func(data []byte) {})
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			lm.Write(mediumMsg)
		}
	})

	b.Run("GetHistory", func(b *testing.B) {
		lm := NewWriter(io.Discard)
		for i := 0; i < 1000; i++ {
			lm.Write(mediumMsg)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			lm.GetHistory()
		}
	})
}
