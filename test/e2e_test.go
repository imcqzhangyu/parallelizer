package test

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"parallelizer/internal/job"
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
func TestDryRunTagDuplicatesLikeGNU(t *testing.T) {
	// GNU parallel prints the command once as output and prefixes it with the
	// same command as the tag: "echo hi<TAB>echo hi".
	r := run(t, "echo hi\n", "-j1", "--dry-run", "--tag")
	if r.code != 0 {
		t.Fatal(r.stderr)
	}
	if r.stdout != "echo hi\techo hi\n" {
		t.Fatalf("stdout = %q", r.stdout)
	}
}

// Percentage --halt rules spool the input to a temp file; that file must be
// removed before exiting (it used to leak because of defer + os.Exit).
func TestPercentHaltRemovesSpoolFile(t *testing.T) {
	dir := t.TempDir()
	r := runEnv(t, append(os.Environ(), "TMPDIR="+dir),
		"echo a\nexit 2\n", "-j1", "--halt", "now,fail=50%")
	if r.code != 50 { // percentage rule: 1 failed of 2 jobs = 50
		t.Fatalf("exit = %d, want 50 (1 failed of 2 jobs)", r.code)
	}
	left, err := filepath.Glob(filepath.Join(dir, "*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("temp files leaked: %v", left)
	}
}

// A bad --tmpdir is rejected up front, before any job runs.
func TestBadTmpdirRejectedAtStartup(t *testing.T) {
	r := run(t, "echo hi\n", "-j1", "--tmpdir", filepath.Join(t.TempDir(), "no-such-dir"))
	if r.code != 255 {
		t.Fatalf("exit = %d, want 255", r.code)
	}
	if !strings.Contains(r.stderr, "not usable") {
		t.Fatalf("stderr = %q, want the --tmpdir problem reported", r.stderr)
	}
}

func TestNegativeMemLimitRejected(t *testing.T) {
	r := run(t, "echo hi\n", "-j1", "--mem-limit", "-1")
	if r.code != 255 {
		t.Fatalf("exit = %d, want 255", r.code)
	}
	if !strings.Contains(r.stderr, "mem-limit") {
		t.Fatalf("stderr = %q", r.stderr)
	}
}

// A read error partway through the command list must fail the run instead of
// silently dropping the remaining commands and exiting 0. Reading a directory
// opens fine on Unix but fails on every read (EISDIR).
func TestInputReadErrorFailsRun(t *testing.T) {
	r := run(t, "", "-j1", "-a", t.TempDir())
	if r.code != 255 {
		t.Fatalf("exit = %d, want 255", r.code)
	}
	if !strings.Contains(r.stderr, "cannot read input") {
		t.Fatalf("stderr = %q, want the read error reported", r.stderr)
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
		t.Fatal("expected a non-zero status after SIGTERM")
	} else if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 143 {
		t.Errorf("exit = %v, want 143", err)
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

// --joblog must not be touched at all by a dry-run, matching GNU parallel
// (which does not even open the file).
func TestJoblogSkipsDryRun(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "joblog")
	r := run(t, "echo hi\n", "-j1", "--dry-run", "--joblog", logPath)
	if r.code != 0 {
		t.Fatal(r.stderr)
	}
	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		t.Fatalf("joblog must not be created during a dry-run (stat err = %v)", err)
	}
}

// signalExit runs a long-running job, sends sig, and asserts the documented
// exit status plus prompt shutdown.
func signalExit(t *testing.T, sig syscall.Signal, want int) {
	t.Helper()
	cmd := exec.Command(binPath, "-j2")
	cmd.Stdin = strings.NewReader("sleep 30\nsleep 30\n")
	cmd.Stdout = io.Discard
	var errb syncBuf
	cmd.Stderr = &errb
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond) // let both jobs start
	start := time.Now()
	if err := cmd.Process.Signal(sig); err != nil {
		t.Fatal(err)
	}
	err := cmd.Wait()
	dur := time.Since(start)

	ee, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("wait: %v (stderr=%s)", err, errb.String())
	}
	if ee.ExitCode() != want {
		t.Fatalf("exit = %d, want %d (stderr=%s)", ee.ExitCode(), want, errb.String())
	}
	if dur > 5*time.Second {
		t.Fatalf("jobs were not killed promptly, took %v", dur)
	}
}

// SIGINT must exit 130 and kill the running jobs.
func TestInterruptExits130(t *testing.T) { signalExit(t, syscall.SIGINT, 130) }

// SIGTERM must exit 143 and kill the running jobs.
func TestTerminateExits143(t *testing.T) { signalExit(t, syscall.SIGTERM, 143) }

// -a FILE reads commands from the file, stdin ignored.
func TestArgFileHappyPath(t *testing.T) {
	file := filepath.Join(t.TempDir(), "cmds")
	if err := os.WriteFile(file, []byte("echo from-a\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r := run(t, "echo from-stdin\n", "-j1", "-a", file)
	if r.code != 0 {
		t.Fatal(r.stderr)
	}
	if r.stdout != "from-a\n" {
		t.Fatalf("stdout = %q, want only the file's command output", r.stdout)
	}
}

// --shell selects the interpreter, and PARALLELIZER_SHELL is the env fallback.
func TestCustomShell(t *testing.T) {
	script := filepath.Join(t.TempDir(), "fakesh")
	body := "#!/bin/sh\necho \"SHELL[$#]:$2\"\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}

	r := run(t, "echo hi\n", "-j1", "--shell", script)
	if r.code != 0 {
		t.Fatal(r.stderr)
	}
	if r.stdout != "SHELL[2]:echo hi\n" {
		t.Fatalf("--shell ignored: stdout = %q", r.stdout)
	}

	r2 := runEnv(t, append(os.Environ(), "PARALLELIZER_SHELL="+script), "echo hi\n", "-j1")
	if r2.stdout != "SHELL[2]:echo hi\n" {
		t.Fatalf("PARALLELIZER_SHELL ignored: stdout = %q", r2.stdout)
	}
}

// --keep-cr keeps the CR of CRLF input (so the shell sees it; here echo
// prints it back).
func TestKeepCR(t *testing.T) {
	r := run(t, "echo A\r\n", "-j1", "--keep-cr")
	if r.code != 0 {
		t.Fatalf("exit = %d, stderr=%s", r.code, r.stderr)
	}
	if r.stdout != "A\r\n" {
		t.Fatalf("stdout = %q, want the CR kept", r.stdout)
	}
}

// --joblog - writes the log to stdout; the header comes first.
func TestJoblogToStdout(t *testing.T) {
	r := run(t, "echo hi\n", "-j1", "--joblog", "-")
	if r.code != 0 {
		t.Fatal(r.stderr)
	}
	lines := strings.Split(strings.TrimRight(r.stdout, "\n"), "\n")
	if !strings.HasPrefix(lines[0], "Seq\tHost\t") {
		t.Fatalf("first stdout line = %q, want the joblog header", lines[0])
	}
	found := false
	for _, l := range lines {
		if strings.HasPrefix(l, "1\t:\t") && strings.HasSuffix(l, "\techo hi") {
			found = true
		}
	}
	if !found {
		t.Fatalf("stdout = %q, want a joblog row for seq 1", r.stdout)
	}
	if !strings.Contains(r.stdout, "hi\n") {
		t.Fatalf("stdout = %q, want the job's output too", r.stdout)
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

// --version and --help work without reading any input.
func TestVersionAndHelp(t *testing.T) {
	r := run(t, "", "--version")
	if r.code != 0 || !strings.Contains(r.stdout, "parallelizer") {
		t.Fatalf("--version: code=%d stdout=%q", r.code, r.stdout)
	}
	r = run(t, "", "--help")
	if r.code != 0 || !strings.Contains(r.stdout, "Usage:") || !strings.Contains(r.stdout, "--retries") {
		t.Fatalf("--help: code=%d stdout=%q", r.code, r.stdout[:min(len(r.stdout), 80)])
	}
}

// -r is GNU's --no-run-if-empty, not --retries.
func TestGNUNoRunIfEmptyFlag(t *testing.T) {
	r := run(t, "echo hi\n", "-r")
	if r.code != 0 || r.stdout != "hi\n" {
		t.Fatalf("code=%d stdout=%q stderr=%q", r.code, r.stdout, r.stderr)
	}
}

// GNU's bare fractional seconds must be accepted (--timeout 0.2).
func TestTimeoutFractionalSeconds(t *testing.T) {
	start := time.Now()
	r := run(t, "sleep 5\n", "-j1", "--timeout", "0.2")
	if r.code != 1 {
		t.Fatalf("exit = %d, want 1 failed job (stderr=%q)", r.code, r.stderr)
	}
	if dur := time.Since(start); dur > 3*time.Second {
		t.Fatalf("timeout did not fire: took %v", dur)
	}
}

// A percentage --halt rule exits with the percentage of failed jobs, as GNU
// parallel does ("1 of 4" = 25), not with the raw failure count.
func TestPercentHaltExitCodeIsPercentage(t *testing.T) {
	r := run(t, "false\nfalse\nfalse\nfalse\n", "-j4", "--halt", "5%")
	if r.code != 25 {
		t.Fatalf("exit = %d, want 25 (one failed of four jobs)", r.code)
	}
}

// The percentage exit status applies even when the rule never fires.
func TestPercentHaltExitWhenRuleNeverFires(t *testing.T) {
	r := run(t, "exit 1\ntrue\ntrue\ntrue\n", "-j1", "--halt", "100%")
	if r.code != 25 {
		t.Fatalf("exit = %d, want 25 (one failed of four jobs, rule never fired)", r.code)
	}
}

// A job that leaves a daemon holding stdout must not hold the run hostage:
// GNU parallel returns when the job exits, and cmd.WaitDelay makes us do the
// same instead of waiting for the orphan to close the inherited pipe.
func TestDaemonGrandchildDoesNotBlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binPath, "-j1")
	cmd.Stdin = strings.NewReader("(trap \"\" HUP; sleep 5 &); echo done\n")
	var out, errb strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errb
	start := time.Now()
	err := cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("run blocked on the daemon's inherited stdout; output=%q", out.String())
	}
	if err != nil {
		t.Fatalf("exit: %v (stderr=%q)", err, errb.String())
	}
	if dur := time.Since(start); dur > 4*time.Second {
		t.Fatalf("run took %v; the orphaned pipe was not bounded", dur)
	}
	if !strings.Contains(out.String(), "done") {
		t.Fatalf("output = %q, want the job's own output", out.String())
	}
}

// SIGINT while the input is being spooled (percentage --halt reads the whole
// input first) must remove the partial spool file and keep the 130 convention
// (the process re-raises the signal with default disposition).
func TestSpoolInterruptCleansUp(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(binPath, "-j1", "--halt", "5%")
	cmd.Env = append(os.Environ(), "TMPDIR="+dir)
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdin = pr
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pr.Close() // the child holds its own end
	if _, err := pw.Write([]byte("echo a\n")); err != nil {
		t.Fatal(err)
	}
	defer pw.Close()

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	time.Sleep(300 * time.Millisecond) // let it block reading the open pipe
	if err := cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}

	var werr error
	select {
	case werr = <-done:
	case <-time.After(time.Second):
		// The signal may have arrived before the spool watcher was installed;
		// a second one is handled by it.
		if err := cmd.Process.Signal(syscall.SIGINT); err != nil {
			t.Fatal(err)
		}
		select {
		case werr = <-done:
		case <-time.After(5 * time.Second):
			cmd.Process.Kill()
			t.Fatal("spooling was not interrupted")
		}
	}

	ee, ok := werr.(*exec.ExitError)
	if !ok {
		t.Fatalf("wait: %v", werr)
	}
	ws, _ := ee.Sys().(syscall.WaitStatus)
	if code := ee.ExitCode(); code != 130 && !(ws.Signaled() && ws.Signal() == syscall.SIGINT) {
		t.Fatalf("exit = %d (%v), want 130 or death by SIGINT", code, ws)
	}
	left, err := filepath.Glob(filepath.Join(dir, "*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("spool file leaked: %v", left)
	}
}

// findPython returns an interactive Python interpreter or skips the test.
func findPython(t *testing.T) string {
	t.Helper()
	for _, cand := range []string{"python3", "python"} {
		if err := exec.Command(cand, "-c", "pass").Run(); err == nil {
			return cand
		}
	}
	t.Skip("no python interpreter for the REPL test")
	return ""
}

// --pty end to end: keys typed by the user reach an interactive REPL running
// in a job, and its output comes back through the normal output pipeline.
//
// The tool's stdin is a pty here (commands come from -a), which exercises the
// stdin-fallback key source. When this test process itself has a controlling
// terminal the tool would use /dev/tty instead (the primary path, covered by
// the manual harness), so skip rather than test the wrong source.
func TestPtyForwardsKeystrokes(t *testing.T) {
	if f, err := os.OpenFile("/dev/tty", os.O_RDWR, 0); err == nil {
		f.Close()
		t.Skip("test process has a controlling terminal; key source would be /dev/tty")
	}
	py := findPython(t)

	dir := t.TempDir()
	cmds := filepath.Join(dir, "cmds")
	if err := os.WriteFile(cmds, []byte(py+" -q\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	master, slave, err := job.OpenPty()
	if err != nil {
		t.Fatalf("OpenPty: %v", err)
	}
	defer master.Close()

	cmd := exec.Command(binPath, "-a", cmds, "-j1", "--pty")
	cmd.Stdin = slave // a terminal: the hub's key source
	var errb strings.Builder
	cmd.Stderr = &errb
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	slave.Close() // the tool holds its own reference

	// Collect the REPL prompt, then type into it.
	var seen strings.Builder
	prompt := make(chan struct{}, 1)
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := stdout.Read(buf)
			if n > 0 {
				seen.Write(buf[:n])
				if strings.Contains(seen.String(), ">>>") {
					select {
					case prompt <- struct{}{}:
					default:
					}
				}
			}
			if err != nil {
				return
			}
		}
	}()

	select {
	case <-prompt:
	case <-time.After(20 * time.Second):
		cmd.Process.Kill()
		t.Fatalf("no REPL prompt; output so far %q, stderr %q", seen.String(), errb.String())
	}
	if _, err := master.Write([]byte("print(40 + 2)\nexit()\n")); err != nil {
		t.Fatalf("typing into the REPL failed: %v", err)
	}

	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	select {
	case err := <-wait:
		if err != nil {
			t.Fatalf("tool exit: %v (stderr=%s)", err, errb.String())
		}
	case <-time.After(30 * time.Second):
		cmd.Process.Kill()
		t.Fatalf("tool did not finish; output %q", seen.String())
	}

	if !strings.Contains(seen.String(), "42") {
		t.Fatalf("REPL never evaluated the typed expression; output=%q stderr=%q",
			seen.String(), errb.String())
	}
}

const joblogHeader = "Seq\tHost\tStarttime\tJobRuntime\tSend\tReceive\tExitval\tSignal\tCommand\n"

// --resume must not re-run (or reprint) jobs already listed in the joblog,
// must append without repeating the header, and must keep --keep-order moving
// over the skipped sequence numbers.
func TestResumeSkipsCompletedJobs(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "joblog")
	seed := joblogHeader + "1\t:\t1.0\t0.1\t0\t0\t0\t0\techo one\n"
	if err := os.WriteFile(logPath, []byte(seed), 0o644); err != nil {
		t.Fatal(err)
	}

	r := run(t, "echo one\necho two\necho three\n", "-j2", "-k", "--joblog", logPath, "--resume")
	if r.code != 0 {
		t.Fatalf("exit = %d, stderr=%s", r.code, r.stderr)
	}
	if r.stdout != "two\nthree\n" {
		t.Fatalf("stdout = %q, want seq 1 skipped and the rest printed in order", r.stdout)
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(data), "Seq\tHost"); n != 1 {
		t.Fatalf("header written %d times, want 1", n)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 4 {
		t.Fatalf("joblog lines = %d, want header + 3 rows:\n%s", len(lines), data)
	}
	if !strings.Contains(lines[1], "echo one") {
		t.Fatalf("seeded row missing:\n%s", data)
	}
	// Rows are written in completion order (as GNU parallel does), so the two
	// new rows may appear in either order.
	joined := strings.Join(lines[2:], "\n")
	if !strings.Contains(joined, "echo two") || !strings.Contains(joined, "echo three") {
		t.Fatalf("joblog rows wrong:\n%s", data)
	}
}

func TestResumeRequiresJoblog(t *testing.T) {
	r := run(t, "echo hi\n", "--resume")
	if r.code != 255 {
		t.Fatalf("exit = %d, want 255", r.code)
	}
	if !strings.Contains(r.stderr, "--joblog") {
		t.Fatalf("stderr = %q, want --resume's requirement spelled out", r.stderr)
	}
}

// --retries N runs a failed job up to N extra times and keeps the last attempt.
func TestRetriesEventuallySucceed(t *testing.T) {
	flag := filepath.Join(t.TempDir(), "flag")
	cmd := fmt.Sprintf("if [ -f %s ]; then echo second; exit 0; else touch %s; echo first; exit 1; fi",
		flag, flag)
	r := run(t, cmd+"\n", "-j1", "--retries", "2")
	if r.code != 0 {
		t.Fatalf("exit = %d, want 0 after a successful retry", r.code)
	}
	for _, want := range []string{"first", "second"} {
		if !strings.Contains(r.stdout, want) {
			t.Fatalf("stdout = %q, missing %q", r.stdout, want)
		}
	}
}

func TestRetriesExhausted(t *testing.T) {
	counter := filepath.Join(t.TempDir(), "attempts")
	r := run(t, fmt.Sprintf("echo x >> %s; exit 1\n", counter), "-j1", "--retries", "2")
	if r.code != 1 {
		t.Fatalf("exit = %d, want 1", r.code)
	}
	data, err := os.ReadFile(counter)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(data), "x"); n != 3 {
		t.Fatalf("attempts = %d, want 1 + 2 retries", n)
	}
}
