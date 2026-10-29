package job

import (
	"bytes"
	"os"
	"testing"
	"time"
)

func newTestHub() *TtyHub {
	// A hub without a key source: tests drive broadcast/Add directly instead
	// of probing a real terminal.
	return &TtyHub{workers: make(map[*os.File]struct{})}
}

func pipeReader(t *testing.T, r *os.File) []byte {
	t.Helper()
	if err := r.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	buf := make([]byte, 256)
	n, err := r.Read(buf)
	if err != nil {
		t.Fatalf("read from pty master: %v", err)
	}
	return buf[:n]
}

// Keys typed while no job is running are queued and flushed to the next job,
// and live keys go straight to running jobs.
func TestTtyHubQueuesThenForwardsKeys(t *testing.T) {
	h := newTestHub()
	h.broadcast([]byte("typed"))
	if string(h.pending) != "typed" {
		t.Fatalf("pending = %q, want the keys buffered while no job runs", h.pending)
	}

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	h.Add(w)
	if len(h.pending) != 0 {
		t.Fatalf("pending keys were not flushed on Add: %q", h.pending)
	}
	if got := pipeReader(t, r); string(got) != "typed" {
		t.Fatalf("new job received %q, want the pending keys", got)
	}

	h.broadcast([]byte("live"))
	if got := pipeReader(t, r); string(got) != "live" {
		t.Fatalf("running job received %q, want live keys", got)
	}

	h.Remove(w)
	h.broadcast([]byte("later"))
	if string(h.pending) != "later" {
		t.Fatalf("pending after Remove = %q, want keys for the next job", h.pending)
	}
	w.Close()
}

// The pending queue is bounded: a huge paste between jobs keeps the newest
// keys (they are the ones meant for the job about to start).
func TestTtyHubPendingQueueKeepsNewestKeys(t *testing.T) {
	h := newTestHub()
	h.broadcast(bytes.Repeat([]byte("a"), pendingCap))
	if len(h.pending) != pendingCap {
		t.Fatalf("pending len = %d, want %d", len(h.pending), pendingCap)
	}
	h.broadcast([]byte("XYZ"))
	if len(h.pending) != pendingCap {
		t.Fatalf("pending len after overflow = %d, want %d", len(h.pending), pendingCap)
	}
	if !bytes.HasSuffix(h.pending, []byte("XYZ")) {
		t.Fatalf("overflow dropped the newest keys: tail = %q", h.pending[len(h.pending)-8:])
	}
	if !bytes.HasPrefix(h.pending, []byte("a")) {
		t.Fatal("overflow should keep a slice of the older keys too")
	}
}

// A closed hub drops keys and refuses new jobs instead of mutating state.
func TestTtyHubAfterClose(t *testing.T) {
	h := newTestHub()
	h.Close()
	h.Close() // idempotent

	h.broadcast([]byte("dropped"))
	if len(h.pending) != 0 {
		t.Fatalf("closed hub queued %q", h.pending)
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	h.Add(w)
	if len(h.workers) != 0 {
		t.Fatalf("closed hub registered a job: %v", h.workers)
	}
}

// Nil hubs are safe everywhere (--pty without a terminal).
func TestTtyHubNilReceiver(t *testing.T) {
	var h *TtyHub
	h.Add(nil)
	h.Remove(nil)
	h.Close()
	if _, ok := h.WindowSize(); ok {
		t.Fatal("nil hub reported a window size")
	}
	h2 := &TtyHub{}
	if _, ok := h2.WindowSize(); ok {
		t.Fatal("hub without a terminal reported a window size")
	}
}
