package test

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var binPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "plz-test-*")
	if err != nil {
		panic(err)
	}
	binPath = filepath.Join(dir, "parallelizer")
	build := exec.Command("go", "build", "-o", binPath, "../cmd/parallelizer")
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		panic(err)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type result struct {
	stdout string
	stderr string
	code   int
	dur    time.Duration
}

func run(t *testing.T, stdin string, args ...string) result {
	t.Helper()
	return runEnv(t, nil, stdin, args...)
}

func runEnv(t *testing.T, env []string, stdin string, args ...string) result {
	t.Helper()
	cmd := exec.Command(binPath, args...)
	cmd.Stdin = strings.NewReader(stdin)
	cmd.Env = env
	var out, errb strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errb
	start := time.Now()
	err := cmd.Run()
	dur := time.Since(start)
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("run: %v", err)
	}
	return result{stdout: out.String(), stderr: errb.String(), code: code, dur: dur}
}

func TestBasic(t *testing.T) {
	r := run(t, "echo one\necho two\necho three\n", "-j2")
	if r.code != 0 {
		t.Fatalf("exit = %d, stderr=%s", r.code, r.stderr)
	}
	for _, want := range []string{"one", "two", "three"} {
		if !strings.Contains(r.stdout, want) {
			t.Errorf("stdout missing %q: %q", want, r.stdout)
		}
	}
}

func TestEmptyInput(t *testing.T) {
	r := run(t, "", "-j4")
	if r.code != 0 || r.stdout != "" {
		t.Fatalf("exit=%d stdout=%q", r.code, r.stdout)
	}
}

func TestHaltExitCode(t *testing.T) {
	r := run(t, "exit 3\n", "-j1", "--halt", "now,fail=1")
	if r.code != 3 {
		t.Fatalf("exit = %d, want 3", r.code)
	}
	if !strings.Contains(r.stderr, "job failed") {
		t.Errorf("stderr = %q", r.stderr)
	}
}

func TestHaltSignalExitCode(t *testing.T) {
	r := run(t, "kill -9 $$\n", "-j1", "--halt", "now,fail=1")
	if r.code != 137 {
		t.Fatalf("exit = %d, want 137", r.code)
	}
}

func TestHaltKillsRunningJobs(t *testing.T) {
	r := run(t, "sleep 5; echo late\nexit 1\n", "-j2", "--halt", "now,fail=1")
	if r.code != 1 {
		t.Fatalf("exit = %d, want 1", r.code)
	}
	if r.dur > 2*time.Second {
		t.Fatalf("took %v; running job was not killed", r.dur)
	}
	if strings.Contains(r.stdout, "late") {
		t.Fatalf("killed job produced output: %q", r.stdout)
	}
}

func TestHaltEscalatesToKill(t *testing.T) {
	r := run(t, "trap '' TERM; sleep 5\nexit 1\n", "-j2", "--halt", "now,fail=1")
	if r.code != 1 {
		t.Fatalf("exit = %d, want 1", r.code)
	}
	if r.dur > 2*time.Second {
		t.Fatalf("took %v; TERM-ignoring job survived", r.dur)
	}
}

func TestSoonWaits(t *testing.T) {
	// The small delay makes sure job 1 is running before the halt fires, so
	// the test does not depend on goroutine scheduling.
	r := run(t, "sleep 0.4; echo done\nsleep 0.05; exit 1\n", "-j2", "--halt", "soon,fail=1")
	if r.code != 1 {
		t.Fatalf("exit = %d, want 1", r.code)
	}
	if !strings.Contains(r.stdout, "done") {
		t.Fatalf("soon should let running jobs finish, stdout=%q", r.stdout)
	}
}

func TestTimeout(t *testing.T) {
	r := run(t, "sleep 10\n", "-j1", "--timeout", "200ms")
	if r.code != 1 {
		t.Fatalf("exit = %d, want 1", r.code)
	}
	if r.dur > 2*time.Second {
		t.Fatalf("timeout did not fire, took %v", r.dur)
	}
}

func TestGroupedOutput(t *testing.T) {
	r := run(t, "echo A1; sleep 0.2; echo A2\necho B1\n", "-j2")
	if r.code != 0 {
		t.Fatal(r.stderr)
	}
	lines := strings.Split(strings.TrimSpace(r.stdout), "\n")
	if len(lines) != 3 {
		t.Fatalf("stdout = %q", r.stdout)
	}
	// A1 and A2 must stay adjacent in either order.
	joined := strings.Join(lines, ",")
	if joined != "A1,A2,B1" && joined != "B1,A1,A2" {
		t.Fatalf("output was interleaved: %q", joined)
	}
}

func TestTag(t *testing.T) {
	r := run(t, "echo hi\n", "-j1", "--tag")
	if r.stdout != "echo hi\thi\n" {
		t.Fatalf("stdout = %q", r.stdout)
	}
}

func TestKeepOrder(t *testing.T) {
	r := run(t, "sleep 0.3; echo first\nsleep 0.05; echo second\n", "-j2", "-k")
	if r.stdout != "first\nsecond\n" {
		t.Fatalf("stdout = %q", r.stdout)
	}
}

func TestCRLF(t *testing.T) {
	r := run(t, "echo A\r\necho B\r\n", "-j1")
	if r.stdout != "A\nB\n" {
		t.Fatalf("stdout = %q", r.stdout)
	}
}

func TestNoTrailingNewline(t *testing.T) {
	r := run(t, "echo last", "-j1")
	if r.stdout != "last\n" {
		t.Fatalf("stdout = %q", r.stdout)
	}
}

func TestStdinIsDevNull(t *testing.T) {
	r := run(t, "wc -c\n", "-j1")
	if strings.TrimSpace(r.stdout) != "0" {
		t.Fatalf("job stdin should be /dev/null, got %q", r.stdout)
	}
}

func TestJoblog(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "joblog")
	r := run(t, "echo hi\nexit 4\n", "-j1", "--joblog", logPath)
	if r.code != 1 {
		t.Fatalf("exit = %d", r.code)
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 3 {
		t.Fatalf("joblog lines = %d", len(lines))
	}
	if lines[0] != "Seq\tHost\tStarttime\tJobRuntime\tSend\tReceive\tExitval\tSignal\tCommand" {
		t.Fatalf("header = %q", lines[0])
	}
	cols := strings.Split(lines[2], "\t")
	if len(cols) != 9 {
		t.Fatalf("columns = %d", len(cols))
	}
	if cols[6] != "4" {
		t.Fatalf("Exitval = %q, want 4", cols[6])
	}
}

func TestDryRun(t *testing.T) {
	r := run(t, "echo a\necho b\n", "-j1", "--dry-run")
	if r.code != 0 {
		t.Fatal(r.stderr)
	}
	if !strings.Contains(r.stdout, "echo a") || !strings.Contains(r.stdout, "echo b") {
		t.Fatalf("stdout = %q", r.stdout)
	}
}

func TestUnlimitedRejected(t *testing.T) {
	r := run(t, "true\n", "-j0")
	if r.code != 255 {
		t.Fatalf("exit = %d, want 255", r.code)
	}
	if !strings.Contains(r.stderr, "unlimited") {
		t.Fatalf("stderr = %q", r.stderr)
	}
}

// Regression: with --keep-order, a job killed by --halt used to leave a hole in
// the sequence and all later output was buffered forever, never printed.
func TestKeepOrderWithKilledJob(t *testing.T) {
	r := run(t, "sleep 3; echo one\nsleep 0.5; exit 1\necho three\n",
		"-j3", "-k", "--halt", "now,fail=1")
	if r.code != 1 {
		t.Fatalf("exit = %d, want 1", r.code)
	}
	if r.dur > 2*time.Second {
		t.Fatalf("killed job was not killed, took %v", r.dur)
	}
	if !strings.Contains(r.stdout, "three") {
		t.Fatalf("output of a later job was lost: stdout=%q", r.stdout)
	}
	if strings.Contains(r.stdout, "one") {
		t.Fatalf("killed job's output must be dropped: stdout=%q", r.stdout)
	}
}

func TestKeepKilledOutput(t *testing.T) {
	r := run(t, "echo kept; sleep 3\nexit 1\n", "-j2",
		"--halt", "now,fail=1", "--keep-killed-output")
	if r.code != 1 {
		t.Fatalf("exit = %d, want 1", r.code)
	}
	if !strings.Contains(r.stdout, "kept") {
		t.Fatalf("--keep-killed-output dropped the output: %q", r.stdout)
	}
}

// --line-buffer must keep stdout and stderr apart.
func TestLineBufferSeparatesStderr(t *testing.T) {
	r := run(t, "echo out; echo err >&2\n", "-j1", "--line-buffer")
	if r.code != 0 {
		t.Fatal(r.stderr)
	}
	if r.stdout != "out\n" {
		t.Fatalf("stdout = %q, want %q", r.stdout, "out\n")
	}
	if !strings.Contains(r.stderr, "err") {
		t.Fatalf("stderr = %q, want the job's stderr", r.stderr)
	}
}

// A single line bigger than --mem-limit must still arrive complete (--line-buffer
// used to buffer it all in memory).
func TestLineBufferHugeLine(t *testing.T) {
	big := strings.Repeat("x", 200000)
	r := run(t, "head -c 200000 /dev/zero | tr '\\0' 'x'\n", "-j1",
		"--line-buffer", "--mem-limit", "1024")
	if r.code != 0 {
		t.Fatal(r.stderr)
	}
	// No trailing newline: the partial line is flushed as-is.
	if r.stdout != big {
		t.Fatalf("stdout = %d bytes, want %d", len(r.stdout), len(big))
	}
}

func TestTagWithUngroup(t *testing.T) {
	r := run(t, "echo hi\n", "-j1", "-u", "--tag")
	if r.stdout != "echo hi\thi\n" {
		t.Fatalf("stdout = %q", r.stdout)
	}
}

// --dry-run output already is the command; --tag must not duplicate it.
func TestDryRunTagDoesNotDuplicate(t *testing.T) {
	r := run(t, "echo hi\n", "-j1", "--dry-run", "--tag")
	if r.code != 0 {
		t.Fatal(r.stderr)
	}
	if r.stdout != "echo hi\n" {
		t.Fatalf("stdout = %q", r.stdout)
	}
}

// Percentage --halt rules spool the input to a temp file; that file must be
// removed before exiting (it used to leak because of defer + os.Exit).
func TestPercentHaltRemovesSpoolFile(t *testing.T) {
	dir := t.TempDir()
	r := runEnv(t, append(os.Environ(), "TMPDIR="+dir),
		"echo a\nexit 2\n", "-j1", "--halt", "now,fail=50%")
	if r.code != 1 {
		t.Fatalf("exit = %d, want 1 failed job", r.code)
	}
	left, err := filepath.Glob(filepath.Join(dir, "*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("temp files leaked: %v", left)
	}
}

// An output stream that cannot be spooled (bad --tmpdir) must fail the job and
// be reported, instead of buffering unbounded in memory and exiting 0.
func TestSpillFailureFailsJob(t *testing.T) {
	r := run(t, "head -c 100000 /dev/zero | tr '\\0' 'x'\n", "-j1",
		"--mem-limit", "1000",
		"--tmpdir", filepath.Join(t.TempDir(), "no-such-dir"))
	if r.code != 1 {
		t.Fatalf("exit = %d, want 1", r.code)
	}
	if !strings.Contains(r.stderr, "cannot spill output") {
		t.Fatalf("stderr = %q, want the spill failure reported", r.stderr)
	}
}

func TestJoblogAppend(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "joblog")
	run(t, "echo one\n", "-j1", "--joblog", logPath)
	run(t, "echo two\n", "-j1", "--joblog", "+"+logPath)

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 3 {
		t.Fatalf("joblog lines = %d, want header + 2 rows:\n%s", len(lines), data)
	}
	if n := strings.Count(string(data), "Seq\tHost"); n != 1 {
		t.Fatalf("header written %d times", n)
	}
	if !strings.Contains(lines[2], "echo two") {
		t.Fatalf("second run missing from log: %q", lines[2])
	}
}

// GNU parallel logs a signal-killed job as Exitval = 128+Signal, Signal = Signal.
func TestJoblogSignalColumns(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "joblog")
	run(t, "kill -9 $$\n", "-j1", "--joblog", logPath)

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	cols := strings.Split(lines[len(lines)-1], "\t")
	if len(cols) != 9 {
		t.Fatalf("columns = %d", len(cols))
	}
	if cols[6] != "137" || cols[7] != "9" {
		t.Fatalf("Exitval/Signal = %q/%q, want 137/9", cols[6], cols[7])
	}
}

// syncBuf is a strings.Builder that can be read while os/exec is still copying
// the child's output into it.
type syncBuf struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func TestSIGUSR1DumpsRunningJobs(t *testing.T) {
	cmd := exec.Command(binPath, "-j1")
	cmd.Stdin = strings.NewReader("sleep 2\n")
	var errb syncBuf
	cmd.Stderr = &errb
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if err := cmd.Process.Signal(syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
		t.Fatal("run ended before the dump could be seen")
	case <-time.After(300 * time.Millisecond):
	}
	if !strings.Contains(errb.String(), "1 job(s) running") {
		t.Errorf("stderr = %q, want the running-job dump", errb.String())
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Error("expected a non-zero status after SIGTERM")
	}
}

// A closed stdout (e.g. `parallelizer ... | head -1`) must exit 141 instead of
// dying from SIGPIPE.
func TestBrokenPipeExits141(t *testing.T) {
	cmd := exec.Command(binPath, "-j2")
	cmd.Stdin = strings.NewReader("seq 1 200000 | sed 's/^/x/'\n")
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout = pw
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pw.Close()
	pr.Close()
	err = cmd.Wait()
	ee, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("wait: %v", err)
	}
	if ee.ExitCode() != 141 {
		t.Fatalf("exit = %d, want 141", ee.ExitCode())
	}
}

// --joblog must not log dry-run jobs, matching GNU parallel.
func TestJoblogSkipsDryRun(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "joblog")
	r := run(t, "echo hi\n", "-j1", "--dry-run", "--joblog", logPath)
	if r.code != 0 {
		t.Fatal(r.stderr)
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(data)) != "Seq\tHost\tStarttime\tJobRuntime\tSend\tReceive\tExitval\tSignal\tCommand" {
		t.Fatalf("dry-run must only write the header, got:\n%s", data)
	}
}

// -vv reports each job's exit status.
func TestVerboseTwice(t *testing.T) {
	r := run(t, "exit 0\n", "-j1", "-v", "-v")
	if r.code != 0 {
		t.Fatal(r.stderr)
	}
	if !strings.Contains(r.stderr, "exit=0") {
		t.Fatalf("stderr = %q, want a completion line", r.stderr)
	}
}
