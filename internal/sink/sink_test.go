package sink

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestSinkMemory(t *testing.T) {
	s := New(t.TempDir(), 1<<20)
	if _, err := s.Write([]byte("hello ")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("world")); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := s.CopyTo(&buf); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "hello world" {
		t.Fatalf("got %q", buf.String())
	}
	if s.Spilled() {
		t.Fatal("should not spill under the limit")
	}
}

func TestSinkSpill(t *testing.T) {
	s := New(t.TempDir(), 16)
	payload := strings.Repeat("x", 100)
	if _, err := s.Write([]byte(payload)); err != nil {
		t.Fatal(err)
	}
	if !s.Spilled() {
		t.Fatal("expected spill")
	}
	var buf bytes.Buffer
	if err := s.CopyTo(&buf); err != nil {
		t.Fatal(err)
	}
	if buf.String() != payload {
		t.Fatalf("got %d bytes, want %d", buf.Len(), len(payload))
	}
	if s.Size() != int64(len(payload)) {
		t.Fatalf("Size = %d, want %d", s.Size(), len(payload))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// A sink that cannot spool to disk must report the failure instead of quietly
// keeping everything in memory (which would defeat --mem-limit and can OOM).
func TestSinkSpillFailureIsAnError(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "no-such-dir"), 16)
	if _, err := s.Write([]byte(strings.Repeat("x", 100))); err == nil {
		t.Fatal("spilling into a missing directory must fail")
	}
	if s.Err() == nil {
		t.Fatal("Err() must report the spill failure")
	}
	// Every later write keeps failing loudly instead of buffering again.
	if _, err := s.Write([]byte("more")); err == nil {
		t.Fatal("writes after a spill failure must fail too")
	}
	if s.Spilled() {
		t.Fatal("nothing should have been spilled")
	}
}
