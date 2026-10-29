package sink

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

// DefaultLimit is how much output is kept in memory before spilling to disk.
const DefaultLimit int64 = 8 << 20

// Sink buffers one job's output, spilling to a temp file past the limit.
type Sink struct {
	limit  int64
	tmpdir string
	buf    bytes.Buffer
	file   *os.File
	n      int64
	err    error // first I/O error; every later Write reports it
}

func New(tmpdir string, limit int64) *Sink {
	if limit <= 0 {
		limit = DefaultLimit
	}
	return &Sink{limit: limit, tmpdir: tmpdir}
}

func (s *Sink) Write(p []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	s.n += int64(len(p))

	if s.file == nil && int64(s.buf.Len())+int64(len(p)) > s.limit {
		// Spill to disk. Failing here is an error: quietly keeping the data in
		// memory would silently defeat --mem-limit and can OOM the machine.
		f, err := os.CreateTemp(s.tmpdir, "prlz-*")
		if err != nil {
			return s.fail(fmt.Errorf("cannot spill output to %s: %w", s.tmpdir, err))
		}
		if s.buf.Len() > 0 {
			if _, werr := f.Write(s.buf.Bytes()); werr != nil {
				f.Close()
				os.Remove(f.Name())
				return s.fail(werr)
			}
			s.buf.Reset()
		}
		s.file = f
	}
	if s.file != nil {
		if _, err := s.file.Write(p); err != nil {
			return s.fail(err)
		}
		return len(p), nil
	}
	if _, err := s.buf.Write(p); err != nil {
		return s.fail(err)
	}
	return len(p), nil
}

func (s *Sink) fail(err error) (int, error) {
	s.err = err
	return 0, err
}

// Err reports the first I/O error, if any.
func (s *Sink) Err() error { return s.err }

func (s *Sink) Size() int64 { return s.n }

// Spilled reports whether the output was written to disk.
func (s *Sink) Spilled() bool { return s.file != nil }

// CopyTo replays the buffered output into w.
func (s *Sink) CopyTo(w io.Writer) error {
	if s.file != nil {
		if _, err := s.file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		_, err := io.Copy(w, s.file)
		return err
	}
	_, err := w.Write(s.buf.Bytes())
	return err
}

// Close releases the temp file, if any.
func (s *Sink) Close() error {
	if s.file == nil {
		return nil
	}
	name := s.file.Name()
	err := s.file.Close()
	os.Remove(name)
	s.file = nil
	return err
}
