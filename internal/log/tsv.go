package log

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"parallelizer/internal/job"
)

// TSV writes the 9-column job log used by GNU parallel.
type TSV struct {
	mu       sync.Mutex
	w        *bufio.Writer
	f        *os.File
	toStdout bool
}

// New opens the job log. The path is a plain filename (overwritten) or, with
// the GNU syntax, "+filename" to append to an existing log without repeating
// the header. "-" writes the log to stdout.
func New(path string) (*TSV, error) {
	if path == "" {
		return nil, nil
	}
	if path == "-" {
		t := &TSV{w: bufio.NewWriter(os.Stdout), toStdout: true}
		t.Header()
		return t, nil
	}

	name := path
	flags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	if strings.HasPrefix(path, "+") {
		name = path[1:]
		flags = os.O_CREATE | os.O_WRONLY | os.O_APPEND
	}
	f, err := os.OpenFile(name, flags, 0o644)
	if err != nil {
		return nil, fmt.Errorf("cannot create --joblog %s: %w", name, err)
	}
	t := &TSV{w: bufio.NewWriterSize(f, 64*1024), f: f}
	if st, err := f.Stat(); err == nil && st.Size() > 0 {
		// Appending to an existing log: GNU parallel does not repeat the
		// header either.
		return t, nil
	}
	t.Header()
	return t, nil
}

func (t *TSV) Header() {
	t.mu.Lock()
	defer t.mu.Unlock()
	fmt.Fprintln(t.w, "Seq\tHost\tStarttime\tJobRuntime\tSend\tReceive\tExitval\tSignal\tCommand")
	t.flushMaybe()
}

func (t *TSV) Write(r job.Result, command string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Exitval/Signal follow GNU parallel: a job killed by signal S is logged
	// as Exitval = 128+S, Signal = S; a normally exited job as its exit status
	// and Signal = 0.
	fmt.Fprintf(t.w, "%d\t%s\t%.3f\t%.3f\t%d\t%d\t%d\t%d\t%s\n",
		r.Seq, ":", float64(r.Start.UnixNano())/1e9, r.Runtime.Seconds(),
		0, r.Received, r.Status(), r.Signal, command)
	t.flushMaybe()
}

// flushMaybe keeps the stdout job log in step with job output instead of
// dumping it all at the end.
func (t *TSV) flushMaybe() {
	if t.toStdout {
		t.w.Flush()
	}
}

// CompletedSeqs returns the job sequence numbers already present in the job
// log, for --resume. A missing file yields an empty set (nothing to resume).
// The path may use the GNU "+" append prefix, which is ignored when reading.
func CompletedSeqs(path string) (map[int]bool, error) {
	done := make(map[int]bool)
	if path == "" || path == "-" {
		return done, nil
	}
	name := strings.TrimPrefix(path, "+")
	f, err := os.Open(name)
	if err != nil {
		if os.IsNotExist(err) {
			return done, nil
		}
		return nil, fmt.Errorf("cannot read --joblog %s: %w", name, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	first := true
	for sc.Scan() {
		line := sc.Text()
		if first {
			first = false
			if strings.HasPrefix(line, "Seq\t") {
				continue // header
			}
		}
		if line == "" {
			continue
		}
		seq, err := strconv.Atoi(strings.SplitN(line, "\t", 2)[0])
		if err != nil {
			// Be liberal: ignore corrupt lines, resume the rest.
			continue
		}
		done[seq] = true
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("cannot read --joblog %s: %w", name, err)
	}
	return done, nil
}

func (t *TSV) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	err := t.w.Flush()
	if t.f != nil {
		if cerr := t.f.Close(); err == nil {
			err = cerr
		}
	}
	return err
}
