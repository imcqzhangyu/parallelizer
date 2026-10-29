package log

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"parallelizer/internal/job"
)

func lines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimRight(string(data), "\n"), "\n")
}

func TestJoblogHeaderAndRow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "joblog")
	tl, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	tl.Write(job.Result{Seq: 1, Start: time.Unix(100, 0), Runtime: time.Second, Received: 5, ExitCode: 0}, "echo hi")
	if err := tl.Close(); err != nil {
		t.Fatal(err)
	}

	ls := lines(t, path)
	if len(ls) != 2 {
		t.Fatalf("lines = %d, want header + 1 row: %q", len(ls), ls)
	}
	if ls[0] != "Seq\tHost\tStarttime\tJobRuntime\tSend\tReceive\tExitval\tSignal\tCommand" {
		t.Fatalf("header = %q", ls[0])
	}
	cols := strings.Split(ls[1], "\t")
	if len(cols) != 9 {
		t.Fatalf("columns = %d", len(cols))
	}
	if cols[6] != "0" || cols[7] != "0" {
		t.Fatalf("Exitval/Signal = %q/%q", cols[6], cols[7])
	}
}

// GNU parallel logs a job killed by signal S as Exitval = 128+S and Signal = S.
func TestJoblogSignalColumnsMatchGNU(t *testing.T) {
	path := filepath.Join(t.TempDir(), "joblog")
	tl, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	tl.Write(job.Result{Seq: 1, Start: time.Unix(100, 0), ExitCode: -1, Signal: 9}, "kill -9 $$")
	tl.Close()

	ls := lines(t, path)
	if len(ls) != 2 {
		t.Fatalf("lines = %d", len(ls))
	}
	cols := strings.Split(ls[1], "\t")
	if cols[6] != "137" || cols[7] != "9" {
		t.Fatalf("Exitval/Signal = %q/%q, want 137/9", cols[6], cols[7])
	}
}

// GNU's "--joblog +FILE" appends and does not repeat the header.
func TestJoblogAppendSyntax(t *testing.T) {
	path := filepath.Join(t.TempDir(), "joblog")

	tl, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	tl.Write(job.Result{Seq: 1, Start: time.Unix(100, 0)}, "echo one")
	tl.Close()

	tl2, err := New("+" + path)
	if err != nil {
		t.Fatal(err)
	}
	tl2.Write(job.Result{Seq: 2, Start: time.Unix(200, 0)}, "echo two")
	tl2.Close()

	ls := lines(t, path)
	if len(ls) != 3 {
		t.Fatalf("lines = %d, want header + 2 rows: %q", len(ls), ls)
	}
	if n := strings.Count(strings.Join(ls, "\n"), "Seq\tHost"); n != 1 {
		t.Fatalf("header written %d times", n)
	}
	if !strings.Contains(ls[2], "echo two") {
		t.Fatalf("appended row missing: %q", ls[2])
	}
}

func TestJoblogMissingFileError(t *testing.T) {
	if _, err := New(filepath.Join(t.TempDir(), "no", "such", "dir", "joblog")); err == nil {
		t.Fatal("expected an error for an unwritable joblog path")
	}
}
