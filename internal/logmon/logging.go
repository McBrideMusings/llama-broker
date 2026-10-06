package logmon

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mostlygeek/llama-swap/internal/event"
)

const DataEventID = 0x04

type DataEvent struct {
	Data []byte
	// Seq is the sequence number of the Write that produced Data. Subscribe
	// uses it to tell lines already in a history snapshot from new ones.
	Seq uint64
}

func (e DataEvent) Type() uint32 {
	return DataEventID
}

// circularBuffer is a fixed-size circular byte buffer that overwrites
// oldest data when full. It provides O(1) writes and O(n) reads.
type circularBuffer struct {
	data []byte
	head int
	size int
}

func newCircularBuffer(capacity int) *circularBuffer {
	return &circularBuffer{
		data: make([]byte, capacity),
		head: 0,
		size: 0,
	}
}

func (cb *circularBuffer) Write(p []byte) {
	if len(p) == 0 {
		return
	}

	cap := len(cb.data)

	if len(p) >= cap {
		copy(cb.data, p[len(p)-cap:])
		cb.head = 0
		cb.size = cap
		return
	}

	firstPart := cap - cb.head
	if firstPart >= len(p) {
		copy(cb.data[cb.head:], p)
		cb.head = (cb.head + len(p)) % cap
	} else {
		copy(cb.data[cb.head:], p[:firstPart])
		copy(cb.data[:len(p)-firstPart], p[firstPart:])
		cb.head = len(p) - firstPart
	}

	cb.size += len(p)
	if cb.size > cap {
		cb.size = cap
	}
}

func (cb *circularBuffer) GetHistory() []byte {
	if cb.size == 0 {
		return nil
	}

	result := make([]byte, cb.size)
	cap := len(cb.data)

	start := (cb.head - cb.size + cap) % cap

	if start+cb.size <= cap {
		copy(result, cb.data[start:start+cb.size])
	} else {
		firstPart := cap - start
		copy(result[:firstPart], cb.data[start:])
		copy(result[firstPart:], cb.data[:cb.size-firstPart])
	}

	return result
}

type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError

	BufferSize = 100 * 1024
)

type Monitor struct {
	eventbus *event.Dispatcher
	mu       sync.RWMutex
	buffer   *circularBuffer
	bufferMu sync.RWMutex
	// seq counts Writes. It is guarded by bufferMu so a history snapshot and
	// the sequence number of its last line are read together.
	seq uint64

	stdout io.Writer

	// broadcastCh hands log data to a dedicated goroutine that owns the
	// (backpressuring) event bus. Write performs a non-blocking send so that
	// slow subscribers can never stall the upstream process's stdout drain.
	broadcastCh chan DataEvent
	dropped     atomic.Uint64
	// droppedSeq is the largest sequence number among the dropped lines.
	droppedSeq atomic.Uint64

	level      Level
	prefix     string
	timeFormat string
}

func New() *Monitor {
	return NewWriter(os.Stdout)
}

func NewWriter(stdout io.Writer) *Monitor {
	m := &Monitor{
		eventbus:    event.NewDispatcherConfig(1000),
		buffer:      nil,
		stdout:      stdout,
		broadcastCh: make(chan DataEvent, 1024),
		level:       LevelInfo,
		prefix:      "",
		timeFormat:  "",
	}
	go m.broadcastLoop()
	return m
}

func (w *Monitor) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	n, err = w.stdout.Write(p)
	if err != nil {
		return n, err
	}

	w.bufferMu.Lock()
	if w.buffer == nil {
		w.buffer = newCircularBuffer(BufferSize)
	}
	w.buffer.Write(p)
	w.seq++
	seq := w.seq
	w.bufferMu.Unlock()

	bufferCopy := make([]byte, len(p))
	copy(bufferCopy, p)
	select {
	case w.broadcastCh <- DataEvent{Data: bufferCopy, Seq: seq}:
	default:
		// Subscribers (e.g. the web UI log stream) can't keep up. Drop the
		// live broadcast rather than block: for the upstream monitor Write
		// runs on the process's stdout drain, so blocking here stalls
		// llama.cpp itself (issue #875). GetHistory() still has the data for
		// reconnecting clients, and the dropped bytes are reported in-stream
		// below. droppedSeq is raised before dropped so the broadcast loop,
		// which reads dropped first, sees a sequence number covering it.
		for old := w.droppedSeq.Load(); seq > old && !w.droppedSeq.CompareAndSwap(old, seq); old = w.droppedSeq.Load() {
		}
		w.dropped.Add(uint64(len(p)))
	}
	return n, nil
}

func (w *Monitor) GetHistory() []byte {
	w.bufferMu.RLock()
	defer w.bufferMu.RUnlock()
	if w.buffer == nil {
		return nil
	}
	return w.buffer.GetHistory()
}

// Clear releases the buffer memory, making it eligible for GC.
// The buffer will be lazily re-allocated on the next Write.
func (w *Monitor) Clear() {
	w.bufferMu.Lock()
	w.buffer = nil
	w.bufferMu.Unlock()
}

// Subscribe calls live with every line written after the subscription. When
// history is non-nil it first receives the lines written before, on the
// caller's goroutine, before live receives anything. Live delivery to every
// subscriber waits while history runs, so history must not block. The broadcast to
// subscribers lags Write, so Subscribe matches the two by sequence number:
// each line reaches exactly one of them.
func (w *Monitor) Subscribe(history, live func(data []byte)) context.CancelFunc {
	// mu holds live lines back until the history has gone out. Until the
	// snapshot is taken, every delivered line comes from a Write that
	// finished before it, so the snapshot holds it and the line is dropped.
	var mu sync.Mutex
	since := uint64(math.MaxUint64)

	mu.Lock()
	defer mu.Unlock()
	cancel := event.Subscribe(w.eventbus, func(e DataEvent) {
		mu.Lock()
		defer mu.Unlock()
		if e.Seq > since {
			live(e.Data)
		}
	})

	w.bufferMu.RLock()
	var snapshot []byte
	if w.buffer != nil {
		snapshot = w.buffer.GetHistory()
	}
	since = w.seq
	w.bufferMu.RUnlock()

	if history != nil && len(snapshot) != 0 {
		history(snapshot)
	}
	return cancel
}

// broadcastLoop is the only place that publishes to the (backpressuring)
// event bus. If subscribers are slow it blocks here, never on Write. Before
// delivering a message it flushes any pending dropped-byte count as an
// in-stream marker so the UI shows where the gap is.
func (w *Monitor) broadcastLoop() {
	for msg := range w.broadcastCh {
		if dropped := w.dropped.Swap(0); dropped > 0 {
			// A subscriber whose history already holds every dropped line
			// skips the notice.
			notice := fmt.Appendf(nil, "\n— %d bytes dropped —\n", dropped)
			event.Publish(w.eventbus, DataEvent{Data: notice, Seq: w.droppedSeq.Load()})
		}
		event.Publish(w.eventbus, msg)
	}
}

func (w *Monitor) SetPrefix(prefix string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.prefix = prefix
}

func (w *Monitor) SetLogLevel(level Level) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.level = level
}

func (w *Monitor) SetLogTimeFormat(timeFormat string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.timeFormat = timeFormat
}

func (w *Monitor) formatMessage(level string, msg string) []byte {
	prefix := ""
	if w.prefix != "" {
		prefix = fmt.Sprintf("[%s] ", w.prefix)
	}
	timestamp := ""
	if w.timeFormat != "" {
		timestamp = fmt.Sprintf("%s ", time.Now().Format(w.timeFormat))
	}
	return fmt.Appendf(nil, "%s%s[%s] %s\n", timestamp, prefix, level, msg)
}

func (w *Monitor) log(level Level, msg string) {
	if level < w.level {
		return
	}
	w.Write(w.formatMessage(level.String(), msg))
}

func (w *Monitor) Debug(msg string) { w.log(LevelDebug, msg) }
func (w *Monitor) Info(msg string)  { w.log(LevelInfo, msg) }
func (w *Monitor) Warn(msg string)  { w.log(LevelWarn, msg) }
func (w *Monitor) Error(msg string) { w.log(LevelError, msg) }

func (w *Monitor) Debugf(format string, args ...any) {
	w.log(LevelDebug, fmt.Sprintf(format, args...))
}

func (w *Monitor) Infof(format string, args ...any) {
	w.log(LevelInfo, fmt.Sprintf(format, args...))
}

func (w *Monitor) Warnf(format string, args ...any) {
	w.log(LevelWarn, fmt.Sprintf(format, args...))
}

func (w *Monitor) Errorf(format string, args ...any) {
	w.log(LevelError, fmt.Sprintf(format, args...))
}

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}
