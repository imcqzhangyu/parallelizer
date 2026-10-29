package source

import (
	"bufio"
	"io"
	"os"
	"strings"
)

// Reader yields non-empty command lines of arbitrary length.
type Reader struct {
	br     *bufio.Reader
	keepCR bool
	eof    bool
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

// Spool copies all command lines into a temporary file and returns the number
// of jobs. It is used when a percentage --halt rule needs the total count.
func Spool(r io.Reader, keepCR bool, tmpdir string) (*os.File, int, error) {
	f, err := os.CreateTemp(tmpdir, "prlz-spool-*")
	if err != nil {
		return nil, 0, err
	}
	w := bufio.NewWriterSize(f, 64*1024)
	rd := New(r, keepCR)
	count := 0
	for {
		line, ok := rd.Next()
		if !ok {
			break
		}
		count++
		if _, err := w.WriteString(line); err != nil {
			f.Close()
			os.Remove(f.Name())
			return nil, 0, err
		}
		if err := w.WriteByte('\n'); err != nil {
			f.Close()
			os.Remove(f.Name())
			return nil, 0, err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, 0, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, 0, err
	}
	return f, count, nil
}
