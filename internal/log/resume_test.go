package log

import (
	"os"
	"path/filepath"
	"testing"
)

func writeJoblog(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestCompletedSeqs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "joblog")
	writeJoblog(t, path, "Seq\tHost\tStarttime\tJobRuntime\tSend\tReceive\tExitval\tSignal\tCommand\n"+
		"1\t:\t1.0\t0.1\t0\t0\t0\t0\techo one\n"+
		"3\t:\t2.0\t0.1\t0\t0\t4\t0\techo three\n"+
		"garbage line\n"+
		"\n")

	done, err := CompletedSeqs(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(done) != 2 || !done[1] || !done[3] {
		t.Fatalf("done = %v, want seqs {1,3}", done)
	}
	if done[2] {
		t.Fatal("seq 2 was never logged")
	}
}

func TestCompletedSeqsAppendPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "joblog")
	writeJoblog(t, path, "Seq\tHost\tStarttime\tJobRuntime\tSend\tReceive\tExitval\tSignal\tCommand\n"+
		"2\t:\t1.0\t0.1\t0\t0\t0\t0\techo two\n")

	done, err := CompletedSeqs("+" + path)
	if err != nil {
		t.Fatal(err)
	}
	if len(done) != 1 || !done[2] {
		t.Fatalf("done = %v", done)
	}
}

func TestCompletedSeqsMissingFile(t *testing.T) {
	done, err := CompletedSeqs(filepath.Join(t.TempDir(), "does-not-exist"))
	if err != nil {
		t.Fatal(err)
	}
	if len(done) != 0 {
		t.Fatalf("done = %v, want empty", done)
	}
}

func TestCompletedSeqsNoHeaderFile(t *testing.T) {
	// A log whose first line is data (no header) still parses.
	path := filepath.Join(t.TempDir(), "joblog")
	writeJoblog(t, path, "1\t:\t1.0\t0.1\t0\t0\t0\t0\techo one\n")

	done, err := CompletedSeqs(path)
	if err != nil {
		t.Fatal(err)
	}
	if !done[1] {
		t.Fatalf("done = %v", done)
	}
}
