package pool

import (
	"bytes"
	"io"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"parallelizer/internal/sink"
)

func sinkWith(t *testing.T, s string) *sink.Sink {
	t.Helper()
	sk := sink.New(t.TempDir(), 1<<20)
	if _, err := sk.Write([]byte(s)); err != nil {
		t.Fatal(err)
	}
	return sk
}

// Regression test: a job whose output is discarded (killed by --halt, or never
// started) must not block the --keep-order cursor, otherwise all later output
// stays in memory forever and is never printed.
func TestKeepOrderSkippedJobDoesNotBlock(t *testing.T) {
	var out, errb bytes.Buffer
	o := NewOutput(&out, &errb, false, true)

	o.Emit(1, "", sinkWith(t, "one\n"), nil)
	// Job 2 produced nothing: killed or skipped.
	o.Skip(2)
	o.Emit(3, "", sinkWith(t, "three\n"), nil)

	if got := out.String(); got != "one\nthree\n" {
		t.Fatalf("stdout = %q, want %q", got, "one\nthree\n")
	}
	if len(o.pending) != 0 {
		t.Fatalf("%d pending entries leaked", len(o.pending))
	}
}

// Output emitted out of order must wait for the gap to be filled, and a job
// that never emits after the gap opens must not block later Emit calls from
// deadlocking (they only buffer).
func TestKeepOrderHoldsUntilGapFills(t *testing.T) {
	var out, errb bytes.Buffer
	o := NewOutput(&out, &errb, false, true)

	o.Emit(2, "", sinkWith(t, "second\n"), nil)
	if out.Len() != 0 {
		t.Fatalf("output before seq 1 arrived: %q", out.String())
	}
	o.Emit(1, "", sinkWith(t, "first\n"), nil)
	if got := out.String(); got != "first\nsecond\n" {
		t.Fatalf("stdout = %q, want %q", got, "first\nsecond\n")
	}
}

// FlushPending prints finished output that is stuck behind sequence numbers
// which can never arrive: the run halted and abandoned queued or unstarted
// jobs, so the --keep-order cursor would otherwise wait forever and never
// print what did finish.
func TestFlushPendingPrintsStuckOutput(t *testing.T) {
	var out, errb bytes.Buffer
	o := NewOutput(&out, &errb, false, true)

	o.Emit(3, "", sinkWith(t, "three\n"), nil)
	o.Emit(5, "", sinkWith(t, "five\n"), nil)
	o.Skip(4) // a killed job between them
	if out.Len() != 0 {
		t.Fatalf("output appeared before FlushPending: %q", out.String())
	}
	o.FlushPending()
	if got := out.String(); got != "three\nfive\n" {
		t.Fatalf("stdout = %q, want the finished output in order", got)
	}
	if len(o.pending) != 0 {
		t.Fatalf("%d pending entries leaked", len(o.pending))
	}
}

// Without --keep-order there is nothing to flush.
func TestFlushPendingNoopWithoutKeepOrder(t *testing.T) {
	var out, errb bytes.Buffer
	o := NewOutput(&out, &errb, false, false)
	o.Emit(1, "", sinkWith(t, "one\n"), nil)
	o.FlushPending()
	if got := out.String(); got != "one\n" {
		t.Fatalf("stdout = %q", got)
	}
}

func TestKeepOrderSkipIsIdempotent(t *testing.T) {
	var out, errb bytes.Buffer
	o := NewOutput(&out, &errb, false, true)

	o.Skip(1)
	o.Skip(1) // must not move the cursor twice
	o.Emit(2, "", sinkWith(t, "two\n"), nil)
	if got := out.String(); got != "two\n" {
		t.Fatalf("stdout = %q, want %q", got, "two\n")
	}
}

func TestWithoutKeepOrderSkipIsNoop(t *testing.T) {
	var out, errb bytes.Buffer
	o := NewOutput(&out, &errb, false, false)

	o.Skip(1)
	o.Emit(2, "", sinkWith(t, "two\n"), nil)
	if got := out.String(); got != "two\n" {
		t.Fatalf("stdout = %q, want %q", got, "two\n")
	}
}

func TestTagPrefixesEachLine(t *testing.T) {
	var out, errb bytes.Buffer
	o := NewOutput(&out, &errb, true, false)

	o.Emit(1, "cmd", sinkWith(t, "a\nb\n"), sinkWith(t, "e\n"))
	if got := out.String(); got != "cmd\ta\ncmd\tb\n" {
		t.Fatalf("stdout = %q", got)
	}
	if got := errb.String(); got != "cmd\te\n" {
		t.Fatalf("stderr = %q", got)
	}
}

// --tag + --ungroup: every line still gets its prefix. Jobs that write partial
// lines still interleave (as in GNU parallel), but each prefix lands at the
// start of a line.
func TestTaggedSerialisesWrites(t *testing.T) {
	var out bytes.Buffer
	o := NewOutput(&out, &bytes.Buffer{}, true, false)
	tw := o.NewTagged("cmd", &out)

	if _, err := tw.Write([]byte("a\nb\n")); err != nil {
		t.Fatal(err)
	}
	// A second job's tagged write must start on its own line.
	tw2 := o.NewTagged("other", &out)
	if _, err := tw2.Write([]byte("x\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("c\n")); err != nil {
		t.Fatal(err)
	}

	want := "cmd\ta\ncmd\tb\nother\tx\ncmd\tc\n"
	if got := out.String(); got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
}

// --line-buffer writes complete lines and keeps stdout and stderr apart.
func TestLineStreamSeparatesStdoutStderr(t *testing.T) {
	var out, errb bytes.Buffer
	o := NewOutput(&out, &errb, false, false)

	lo := o.NewLineStream("cmd", &out, 1<<20)
	le := o.NewLineStream("cmd", &errb, 1<<20)
	if _, err := lo.Write([]byte("out1\nout2\npartial")); err != nil {
		t.Fatal(err)
	}
	if _, err := le.Write([]byte("err1\n")); err != nil {
		t.Fatal(err)
	}
	lo.Flush()

	if got := out.String(); got != "out1\nout2\npartial" {
		t.Fatalf("stdout = %q", got)
	}
	if got := errb.String(); got != "err1\n" {
		t.Fatalf("stderr = %q", got)
	}
}

// A line longer than --mem-limit must be flushed in pieces instead of being
// buffered in memory without bound.
func TestLineStreamCapsPartialLine(t *testing.T) {
	var out, errb bytes.Buffer
	o := NewOutput(&out, &errb, false, false)
	lo := o.NewLineStream("cmd", &out, 1024)

	big := strings.Repeat("x", 10000)
	if _, err := lo.Write([]byte(big)); err != nil {
		t.Fatal(err)
	}
	if out.Len() == 0 {
		t.Fatal("a partial line bigger than --mem-limit must not stay buffered")
	}
	lo.Flush()
	if got := out.String(); got != big {
		t.Fatalf("output lost: got %d bytes, want %d", len(got), len(big))
	}
}

func TestEPIPECallsHandler(t *testing.T) {
	o := NewOutput(&brokenWriter{}, &bytes.Buffer{}, false, false)
	called := make(chan struct{}, 1)
	o.onEPIPE = func() { called <- struct{}{} }

	o.Emit(1, "", sinkWith(t, strings.Repeat("y", 4096)), nil)
	select {
	case <-called:
	default:
		t.Fatal("EPIPE did not trigger onEPIPE")
	}
}

// A non-EPIPE write error must be reported once, not silently dropped.
func TestWriteErrorIsReported(t *testing.T) {
	var errb bytes.Buffer
	o := NewOutput(&diskFullWriter{}, &errb, false, false)

	o.Emit(1, "", sinkWith(t, "hello\n"), nil)
	o.Emit(2, "", sinkWith(t, "again\n"), nil)

	got := errb.String()
	if !strings.Contains(got, "write error") || !strings.Contains(got, syscall.ENOSPC.Error()) {
		t.Fatalf("stderr = %q, want the write error to be reported", got)
	}
	if n := strings.Count(got, "write error"); n != 1 {
		t.Fatalf("error reported %d times, want 1", n)
	}
}

type brokenWriter struct{}

func (brokenWriter) Write(p []byte) (int, error) { return 0, syscall.EPIPE }

type diskFullWriter struct{}

func (diskFullWriter) Write(p []byte) (int, error) { return 0, syscall.ENOSPC }

func TestFirstSinkErr(t *testing.T) {
	good := sinkWith(t, "ok")
	if err := firstSinkErr(nil, good); err != nil {
		t.Fatalf("healthy sink reported: %v", err)
	}
	if err := firstSinkErr(nil, nil); err != nil {
		t.Fatalf("nil sinks reported: %v", err)
	}

	bad := sink.New(filepath.Join(t.TempDir(), "missing-dir"), 1)
	if _, err := bad.Write([]byte(strings.Repeat("x", 100))); err == nil {
		t.Fatal("expected the spill to fail")
	}
	if err := firstSinkErr(bad, good); err == nil {
		t.Fatal("sink failure must surface to the caller")
	}
}

// Guards the per-line locking cost of --line-buffer: output handling must stay
// far cheaper than spawning the job processes themselves.
func BenchmarkLineStream(b *testing.B) {
	o := NewOutput(io.Discard, io.Discard, false, false)
	ls := o.NewLineStream("cmd", io.Discard, 1<<20)
	line := []byte(strings.Repeat("x", 79) + "\n")
	b.SetBytes(int64(len(line)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ls.Write(line); err != nil {
			b.Fatal(err)
		}
	}
}

// The grouped path holds the output lock while copying a whole job's output.
func BenchmarkEmitGrouped(b *testing.B) {
	o := NewOutput(io.Discard, io.Discard, false, false)
	tmpdir := b.TempDir()
	payload := bytes.Repeat([]byte("payload line\n"), 100) // ~1.3 KB per job
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sk := sink.New(tmpdir, 1<<20)
		sk.Write(payload)
		o.Emit(i+1, "", sk, nil)
	}
}
