package job

// TtyHub forwards keystrokes from our own terminal to the pseudo-terminals of
// running --pty jobs, so interactive programs (TUIs) receive input.
//
// Key source, in order: /dev/tty (the normal case, where stdin carries the
// command list), else stdin when stdin is itself a terminal (e.g. tests that
// attach a pty as stdin). With neither (cron, CI) no keys are forwarded and
// TUIs rely on --timeout.

import (
	"io"
	"os"
	"sync"
)

// pendingCap bounds keys typed while no job is running; they are flushed to
// the next job that starts (the terminal's own queue holds them only until
// we read them).
const pendingCap = 64 << 10

type TtyHub struct {
	mu      sync.Mutex
	workers map[*os.File]struct{}
	pending []byte
	src     io.Reader // /dev/tty or stdin; nil when there is no input source
	srcFile *os.File  // non-nil when we opened /dev/tty ourselves (we close it)
	win     *os.File  // file whose window size seeds new ptys
	started bool
	closed  bool

	// writeMu serialises writes to the pty masters. It is separate from mu so
	// that a master whose input buffer is full (the job is not reading) only
	// blocks the key pump, never Add/Remove/Close.
	writeMu sync.Mutex
}

// NewTtyHub probes our terminal. It never fails: without a terminal the hub
// simply has no input source and no window size.
func NewTtyHub() *TtyHub {
	h := &TtyHub{workers: make(map[*os.File]struct{})}
	// Opening /dev/tty non-blocking does not work reliably: on Darwin the fd
	// cannot be registered with kqueue, so Read returns EAGAIN instead of
	// waiting for keys. A blocking read is fine — the pump is one goroutine
	// that lives as long as the process.
	if f, err := os.OpenFile("/dev/tty", os.O_RDWR, 0); err == nil {
		h.src, h.srcFile, h.win = f, f, f
		return h
	}
	if _, ok := terminalSize(os.Stdin); ok {
		h.src, h.win = os.Stdin, os.Stdin
	}
	return h
}

// WindowSize reports our terminal's size for seeding a new pty.
func (h *TtyHub) WindowSize() (winsize, bool) {
	if h == nil || h.win == nil {
		return winsize{}, false
	}
	return terminalSize(h.win)
}

// Add registers a job's pty master: pending keys are flushed to it and, on
// the first job, the key-reading pump starts.
func (h *TtyHub) Add(master *os.File) {
	if h == nil {
		return
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	flush := h.pending
	h.pending = nil
	h.mu.Unlock()

	// Publish the master under writeMu together with the pending flush so a
	// concurrent broadcast cannot deliver newer keys ahead of older ones.
	h.writeMu.Lock()
	if len(flush) > 0 {
		_, _ = master.Write(flush) // best effort: the job may already be gone
	}
	h.mu.Lock()
	h.workers[master] = struct{}{}
	startPump := !h.started && h.src != nil && !h.closed
	if startPump {
		h.started = true
	}
	h.mu.Unlock()
	h.writeMu.Unlock()

	if startPump {
		go h.pump()
	}
}

// Remove unregisters a job's pty master (call before closing it). Writes to a
// closed *os.File fail without touching the descriptor, so an in-flight
// broadcast that still has this master is harmless.
func (h *TtyHub) Remove(master *os.File) {
	if h == nil {
		return
	}
	h.mu.Lock()
	delete(h.workers, master)
	h.mu.Unlock()
}

// pump copies our terminal's input to every registered job. It reads from one
// source for the process lifetime; concurrent job churn only swaps the
// destination set.
func (h *TtyHub) pump() {
	buf := make([]byte, 4096)
	for {
		n, err := h.src.Read(buf)
		if n > 0 {
			h.broadcast(buf[:n])
		}
		if err != nil {
			return // EOF, or the source was closed at shutdown
		}
		h.mu.Lock()
		closed := h.closed
		h.mu.Unlock()
		if closed {
			return // stdin source: Close() cannot unblock Read itself
		}
	}
}

// broadcast sends keys to all running jobs, or queues them (bounded) when
// none are running so input typed between jobs reaches the next one.
func (h *TtyHub) broadcast(data []byte) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	if len(h.workers) == 0 {
		h.pending = append(h.pending, data...)
		if len(h.pending) > pendingCap {
			// Keep the newest keys: they are for the job about to start.
			h.pending = append(h.pending[:0], h.pending[len(h.pending)-pendingCap:]...)
		}
		h.mu.Unlock()
		return
	}
	targets := make([]*os.File, 0, len(h.workers))
	for w := range h.workers {
		targets = append(targets, w)
	}
	h.mu.Unlock()

	// Writes happen outside h.mu: a job that is not reading can fill its pty
	// input buffer and block here, and that must not freeze Add/Remove/Close.
	h.writeMu.Lock()
	for _, w := range targets {
		_, _ = w.Write(data)
	}
	h.writeMu.Unlock()
}

// Close stops the pump and releases the /dev/tty handle. stdin is never
// closed (it is not ours).
func (h *TtyHub) Close() {
	if h == nil {
		return
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	srcFile := h.srcFile
	h.mu.Unlock()
	if srcFile != nil {
		// A pump parked in a blocking read cannot be woken by Close; it exits
		// with the process. Closing the file is still the right thing to do
		// (the next Read, if any, fails).
		srcFile.Close()
	}
}
