package source

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

// Reader yields non-empty command lines of arbitrary length.
type Reader struct {
	br     *bufio.Reader
	keepCR bool
	eof    bool
	err    error
}

func New(r io.Reader, keepCR bool) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, 64*1024), keepCR: keepCR}
}

// Next returns the next command line. Empty and whitespace-only lines are
// skipped, matching GNU parallel.
func (r *Reader) Next() (string, bool) {
	for !r.eof {
		line, err := r.br.ReadString('\n')
		if err != nil {
			r.eof = true
			// Hitting the end of the input is normal; anything else means the
			// command list was read only partially and must fail the run
			// instead of silently dropping the remaining commands.
			if err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
				r.err = err
			}
		}
		line = strings.TrimSuffix(line, "\n")
		if !r.keepCR {
			line = strings.TrimSuffix(line, "\r")
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		return line, true
	}
	return "", false
}

// Err reports a non-EOF read error seen while streaming the command list.
func (r *Reader) Err() error { return r.err }

// Spool copies all command lines into a temporary file and returns the number
// of jobs. It is used when a percentage --halt rule needs the total count.
//
// If the user interrupts (SIGINT/SIGTERM) while the input is being spooled,
// the partial file is removed and the signal is re-raised with its default
// disposition, so the process still dies with the usual 130/143 status.
func Spool(r io.Reader, keepCR bool, tmpdir string) (*os.File, int, error) {
	f, err := os.CreateTemp(tmpdir, "prlz-spool-*")
	if err != nil {
		return nil, 0, err
	}
	name := f.Name()

	// Remove the temp file on interrupt while we own it. Reset + re-raise
	// keeps the shell-visible exit status (130/143) intact.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	stop := make(chan struct{})
	var once sync.Once
	remove := func() {
		once.Do(func() {
			f.Close()
			os.Remove(name)
		})
	}
	go func() {
		select {
		case sig := <-sigCh:
			remove()
			signal.Reset(sig)
			if s, ok := sig.(syscall.Signal); ok {
				syscall.Kill(syscall.Getpid(), s)
			}
		case <-stop:
		}
	}()
	stopWatching := func() {
		signal.Stop(sigCh)
		close(stop)
	}

	w := bufio.NewWriterSize(f, 64*1024)
	rd := New(r, keepCR)
	count := 0
	fail := func(err error) (*os.File, int, error) {
		stopWatching()
		remove()
		return nil, 0, err
	}
	for {
		line, ok := rd.Next()
		if !ok {
			break
		}
		count++
		if _, err := w.WriteString(line); err != nil {
			return fail(err)
		}
		if err := w.WriteByte('\n'); err != nil {
			return fail(err)
		}
	}
	if err := rd.Err(); err != nil {
		return fail(fmt.Errorf("cannot read input: %w", err))
	}
	if err := w.Flush(); err != nil {
		return fail(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fail(err)
	}
	// Success: hand ownership to the caller; stop watching for signals.
	stopWatching()
	return f, count, nil
}
