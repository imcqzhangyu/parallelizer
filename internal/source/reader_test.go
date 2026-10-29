package source

import (
	"bytes"
	"strings"
	"testing"
)

func lines(in string, keepCR bool) []string {
	r := New(strings.NewReader(in), keepCR)
	var out []string
	for {
		line, ok := r.Next()
		if !ok {
			return out
		}
		out = append(out, line)
	}
}

func TestReader(t *testing.T) {
	tests := []struct {
		name   string
		in     string
		keepCR bool
		want   []string
	}{
		{"simple", "a\nb\n", false, []string{"a", "b"}},
		{"empty lines", "\n\na\n\n \n\t\nb\n", false, []string{"a", "b"}},
		{"crlf", "a\r\nb\r\n", false, []string{"a", "b"}},
		{"keep cr", "a\r\nb\r\n", true, []string{"a\r", "b\r"}},
		{"no trailing newline", "a\nb", false, []string{"a", "b"}},
		{"only whitespace", "  \n\t\n", false, nil},
		{"empty", "", false, nil},
		{"single line no newline", "only", false, []string{"only"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := lines(tc.in, tc.keepCR)
			if len(got) != len(tc.want) {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %q, want %q", got, tc.want)
				}
			}
		})
	}
}

func TestReaderLongLine(t *testing.T) {
	long := strings.Repeat("x", 5<<20)
	got := lines(long+"\n", false)
	if len(got) != 1 || len(got[0]) != len(long) {
		t.Fatalf("long line was truncated: %d bytes", len(got))
	}
}

func TestReaderNUL(t *testing.T) {
	got := lines("a\x00b\n", false)
	if len(got) != 1 || !bytes.Contains([]byte(got[0]), []byte{0}) {
		t.Fatalf("NUL byte should be preserved, got %q", got)
	}
}

func TestSpool(t *testing.T) {
	f, n, err := Spool(strings.NewReader("a\n\nb\nc\n"), false, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if n != 3 {
		t.Fatalf("count = %d, want 3", n)
	}
	buf := make([]byte, 16)
	size, _ := f.Read(buf)
	if string(buf[:size]) != "a\nb\nc\n" {
		t.Fatalf("spooled %q", buf[:size])
	}
}
