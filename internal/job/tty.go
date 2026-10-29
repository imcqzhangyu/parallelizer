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
	winFd   uintptr   // fd to copy the window size from
	started bool
	closed  bool
}

// NewTtyHub probes our terminal. It never fails: without a terminal the hub
// simply has no input source and no window size.
func NewTtyHub() *TtyHub {
	h := &TtyHub{workers: make(map[*os.File]struct{})}
	if f, err := os.OpenFile("/dev/tty", os.O_RDWR, 0); err == nil {
		h.src, h.srcFile, h.winFd = f, f, f.Fd()
		return h
	}
	if _, ok := TerminalSize(os.Stdin.Fd()); ok {
		h.src, h.winFd = os.Stdin, os.Stdin.Fd()
	}
	return h
}

// WindowSize reports our terminal's size for seeding a new pty.
func (h *TtyHub) WindowSize() (winsize, bool) {
	if h == nil || h.winFd == 0 {
		return winsize{}, false
	}
	return TerminalSize(h.winFd)
}

// Add registers a job's pty master: pending keys are flushed to it and, on
// the first job, the key-reading pump starts.
func (h *TtyHub) Add(master *os.File) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.workers[master] = struct{}{}
	flush := h.pending
	h.pending = nil
	startPump := !h.started && h.src != nil && !h.closed
	if startPump {
		h.started = true
	}
	h.mu.Unlock()

	if len(flush) > 0 {
		master.Write(flush) // best effort: the job may already be gone
	}
	if startPump {
		go h.pump()
	}
}

// Remove unregisters a job's pty master (call before closing it).
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
// destination set under the lock.
func (h *TtyHub) pump() {
	buf := make([]byte, 4096)
	for {
		n, err := h.src.Read(buf)
		if n > 0 {
			h.broadcast(buf[:n])
		}
		if err != nil {
			return // EOF or the source was closed at shutdown
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
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	if len(h.workers) == 0 {
		h.pending = append(h.pending, data...)
		if len(h.pending) > pendingCap {
			// Keep the newest keys: they are for the job about to start.
			h.pending = append(h.pending[:0], h.pending[len(h.pending)-pendingCap:]...)
		}
		return
	}
	for w := range h.workers {
		_, _ = w.Write(data)
	}
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
		srcFile.Close() // unblocks a pump stuck in Read
	}
}
