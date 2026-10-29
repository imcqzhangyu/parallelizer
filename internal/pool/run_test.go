package pool

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"parallelizer/internal/cli"
)

func newDryRunPool(t *testing.T, args []string, total int) (*Pool, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	opts, err := cli.Parse(append(args, "--dry-run"))
	if err != nil {
		t.Fatal(err)
	}
	var out, errb bytes.Buffer
	p, err := New(opts, total, &out, &errb)
	if err != nil {
		t.Fatal(err)
	}
	return p, &out, &errb
}

// Covers the scheduling loop, blank-line skipping and the dry-run path of
// runOne (no processes are spawned).
func TestRunDryRunScheduling(t *testing.T) {
	p, out, _ := newDryRunPool(t, []string{"-j1"}, 0)
	code := p.Run(strings.NewReader("echo a\n\necho b\n"))
	if code != 0 {
		t.Fatalf("exit = %d", code)
	}
	if out.String() != "echo a\necho b\n" {
		t.Fatalf("stdout = %q", out.String())
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRunDryRunKeepOrder(t *testing.T) {
	p, out, _ := newDryRunPool(t, []string{"-j4", "-k"}, 0)
	code := p.Run(strings.NewReader("echo one\necho two\necho three\n"))
	if code != 0 {
		t.Fatalf("exit = %d", code)
	}
	if out.String() != "echo one\necho two\necho three\n" {
		t.Fatalf("stdout = %q", out.String())
	}
}

// --resume skips completed sequence numbers and still keeps -k ordered.
func TestRunResumeSkip(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "joblog")
	seed := "Seq\tHost\tStarttime\tJobRuntime\tSend\tReceive\tExitval\tSignal\tCommand\n" +
		"1\t:\t1.0\t0.1\t0\t0\t0\t0\techo a\n"
	if err := os.WriteFile(logPath, []byte(seed), 0o644); err != nil {
		t.Fatal(err)
	}

	p, out, _ := newDryRunPool(t, []string{"-j1", "-k", "--joblog", logPath, "--resume"}, 0)
	code := p.Run(strings.NewReader("echo a\necho b\n"))
	if code != 0 {
		t.Fatalf("exit = %d", code)
	}
	if out.String() != "echo b\n" {
		t.Fatalf("stdout = %q, want seq 1 skipped", out.String())
	}
}

// --retries must re-run a failed job; a shell that does not exist fails every
// attempt without spawning a process.
func TestRunRetriesLoop(t *testing.T) {
	opts, err := cli.Parse([]string{"-j1", "--shell", "/nonexistent/sh", "--retries", "2", "-v"})
	if err != nil {
		t.Fatal(err)
	}
	var out, errb bytes.Buffer
	p, err := New(opts, 0, &out, &errb)
	if err != nil {
		t.Fatal(err)
	}
	code := p.Run(strings.NewReader("true\n"))
	if code != 1 {
		t.Fatalf("exit = %d, want 1 (the failing job counted)", code)
	}
	for i := 1; i <= 2; i++ {
		want := "retry " + itoa(i) + "/2"
		if !strings.Contains(errb.String(), want) {
			t.Fatalf("stderr missing %q:\n%s", want, errb.String())
		}
	}
}

// --resume + percentage --halt: only resumed jobs that also occur in the
// current input (seq <= total) may shrink the total.
func TestResumePercentTotalOverlap(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "joblog")
	seed := "Seq\tHost\tStarttime\tJobRuntime\tSend\tReceive\tExitval\tSignal\tCommand\n" +
		"1\t:\t1.0\t0.1\t0\t0\t0\t0\techo a\n" +
		"3\t:\t1.0\t0.1\t0\t0\t0\t0\techo c\n" +
		"99\t:\t1.0\t0.1\t0\t0\t0\t0\techo beyond\n"
	if err := os.WriteFile(logPath, []byte(seed), 0o644); err != nil {
		t.Fatal(err)
	}

	opts, err := cli.Parse([]string{"--halt", "now,fail=50%", "--joblog", logPath, "--resume"})
	if err != nil {
		t.Fatal(err)
	}
	var out, errb bytes.Buffer
	p, err := New(opts, 10, &out, &errb) // 10 jobs in this input
	if err != nil {
		t.Fatal(err)
	}
	// 10 - {seq 1, seq 3} = 8; seq 99 is not part of this input.
	if p.st.total != 8 {
		t.Fatalf("total = %d, want 8", p.st.total)
	}
}

func itoa(n int) string {
	return string(rune('0' + n))
}
