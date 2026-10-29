package pool

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"syscall"

	"parallelizer/internal/sink"
)

// Output serialises job output onto stdout/stderr.
type Output struct {
	mu        sync.Mutex
	out       io.Writer
	errw      io.Writer
	tag       bool
	keepOrder bool
	next      int
	pending   map[int]*pending
	errOnce   sync.Once

	onEPIPE func()
}

// pending holds one finished job's sinks; a nil entry means "this job produced
// no output" (killed by --halt, --dry-run job, ...) and only exists to keep the
// --keep-order cursor moving over the gap.
type pending struct {
	tag string
	a   *sink.Sink
	b   *sink.Sink
}

func NewOutput(out, errw io.Writer, tag, keepOrder bool) *Output {
	return &Output{
		out:       out,
		errw:      errw,
		tag:       tag,
		keepOrder: keepOrder,
		next:      1,
		pending:   make(map[int]*pending),
	}
}

// Emit writes one finished job's output. tag is the --tag prefix (the job's
// command line, matching GNU parallel).
func (o *Output) Emit(seq int, tag string, a, b *sink.Sink) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.keepOrder {
		o.writeLocked(tag, a, b)
		return
	}
	o.pending[seq] = &pending{tag: tag, a: a, b: b}
	o.drainPendingLocked()
}

// Skip records that a job's output is intentionally not printed (killed by
// --halt, or a job that was never started). Without it, --keep-order would
// wait forever for a sequence number that is never emitted and all later
// output would be stuck in memory.
func (o *Output) Skip(seq int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.keepOrder {
		return
	}
	if _, exists := o.pending[seq]; exists {
		return
	}
	o.pending[seq] = nil
	o.drainPendingLocked()
}

// drainPendingLocked emits pending jobs in sequence order.
// Callers must hold o.mu.
func (o *Output) drainPendingLocked() {
	for {
		p, ok := o.pending[o.next]
		if !ok {
			break
		}
		delete(o.pending, o.next)
		o.next++
		if p != nil {
			o.writeLocked(p.tag, p.a, p.b)
		}
	}
}

func (o *Output) writeLocked(tag string, a, b *sink.Sink) {
	// An empty tag (--dry-run) must not leave a bare TAB behind.
	if o.tag && tag != "" {
		prefix := []byte(tag + "\t")
		if a != nil {
			tw := &tagWriter{w: o.out, prefix: prefix, bol: true}
			o.handleErr(a.CopyTo(tw))
		}
		if b != nil {
			tw := &tagWriter{w: o.errw, prefix: prefix, bol: true}
			o.handleErr(b.CopyTo(tw))
		}
	} else {
		if a != nil {
			o.handleErr(a.CopyTo(o.out))
		}
		if b != nil {
			o.handleErr(b.CopyTo(o.errw))
		}
	}
	if a != nil {
		a.Close()
	}
	if b != nil {
		b.Close()
	}
}

// handleErr reacts to a write failure on our own output. A broken pipe stops
// the run like `head` on a GNU parallel pipeline; anything else (disk full,
// ...) is reported once instead of being silently dropped, because the user
// would otherwise see a successful exit status for truncated output.
func (o *Output) handleErr(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, syscall.EPIPE) && o.onEPIPE != nil {
		o.onEPIPE()
		return
	}
	o.errOnce.Do(func() {
		fmt.Fprintf(o.errw, "parallelizer: write error: %v\n", err)
	})
}

// LineStream turns a byte stream into line-atomic writes (--line-buffer).
type LineStream struct {
	o   *Output
	w   io.Writer // stdout or stderr
	tag string
	buf []byte
	max int64 // partial-line cap; 0 = unlimited
}

// NewLineStream returns a line-buffered stream written to w, which must be
// the same writer passed to the Pool (stdout or stderr).
func (o *Output) NewLineStream(tag string, w io.Writer, max int64) *LineStream {
	return &LineStream{o: o, w: w, tag: tag, max: max}
}

func (l *LineStream) Write(p []byte) (int, error) {
	l.buf = append(l.buf, p...)
	l.drain(false)
	return len(p), nil
}

func (l *LineStream) drain(all bool) {
	for {
		i := bytes.IndexByte(l.buf, '\n')
		if i < 0 {
			break
		}
		l.o.writeLine(l.w, l.tag, l.buf[:i+1])
		l.buf = l.buf[i+1:]
	}
	// A line without a newline must not grow without bound: flush the partial
	// line once it exceeds the memory limit (lines are then no longer atomic).
	if (all || (l.max > 0 && int64(len(l.buf)) > l.max)) && len(l.buf) > 0 {
		l.o.writeLine(l.w, l.tag, l.buf)
		l.buf = nil
	}
}

// Flush emits any trailing partial line.
func (l *LineStream) Flush() { l.drain(true) }

func (o *Output) writeLine(w io.Writer, tag string, line []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.tag && tag != "" {
		if _, err := io.WriteString(w, tag+"\t"); err != nil {
			o.handleErr(err)
		}
	}
	if _, err := w.Write(line); err != nil {
		o.handleErr(err)
	}
}

// Tagged prefixes every write with tag + TAB and serialises jobs' writes so
// prefixes stay line-atomic. It is what makes --tag work with --ungroup.
type Tagged struct {
	o  *Output
	tw *tagWriter
}

func (o *Output) NewTagged(tag string, w io.Writer) *Tagged {
	return &Tagged{o: o, tw: &tagWriter{w: w, prefix: []byte(tag + "\t"), bol: true}}
}

func (t *Tagged) Write(p []byte) (int, error) {
	t.o.mu.Lock()
	defer t.o.mu.Unlock()
	n, err := t.tw.Write(p)
	t.o.handleErr(err)
	return n, err
}

type tagWriter struct {
	w      io.Writer
	prefix []byte
	bol    bool
}

func (t *tagWriter) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		if t.bol {
			if _, err := t.w.Write(t.prefix); err != nil {
				return total, err
			}
			t.bol = false
		}
		i := bytes.IndexByte(p, '\n')
		var chunk []byte
		if i < 0 {
			chunk, p = p, nil
		} else {
			chunk, p = p[:i+1], p[i+1:]
			t.bol = true
		}
		n, err := t.w.Write(chunk)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// counter measures how many bytes a job produced.
type counter struct {
	w io.Writer
	n int64
}

func (c *counter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
