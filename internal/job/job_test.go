package job

import (
	"bytes"
	"errors"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func testExec() *Exec {
	return &Exec{
		Shell:   "/bin/sh",
		TermSeq: []SignalStep{{Signal: syscall.SIGTERM, Wait: 50 * time.Millisecond}, {Signal: syscall.SIGKILL, Wait: 50 * time.Millisecond}},
	}
}

func TestRunSuccess(t *testing.T) {
	var out, errb bytes.Buffer
	var started []int

	e := testExec()
	e.OnStart = func(seq, pgid int) { started = append(started, pgid) }

	res := e.Run(Spec{Seq: 1, Command: "echo hi; echo err >&2"}, &out, &errb)
	if res.Failed() {
		t.Fatalf("job failed: %+v", res)
	}
	if res.ExitCode != 0 || res.Signal != 0 {
		t.Fatalf("exit/signal = %d/%d", res.ExitCode, res.Signal)
	}
	if out.String() != "hi\n" || errb.String() != "err\n" {
		t.Fatalf("stdout=%q stderr=%q", out.String(), errb.String())
	}
	if res.Received != 0 { // set by the pool, not Exec
		t.Fatal("Exec should not set Received")
	}
	if len(started) != 1 || started[0] <= 1 {
		t.Fatalf("OnStart pids = %v, want one pid > 1", started)
	}
	if res.Runtime <= 0 || res.Start.IsZero() {
		t.Fatalf("timing not recorded: %+v", res)
	}
	if res.Status() != 0 {
		t.Fatalf("Status = %d", res.Status())
	}
}

func TestRunFailureExitCode(t *testing.T) {
	var out, errb bytes.Buffer
	res := testExec().Run(Spec{Seq: 1, Command: "exit 3"}, &out, &errb)
	if !res.Failed() {
		t.Fatal("exit 3 must count as a failure")
	}
	if res.ExitCode != 3 || res.Signal != 0 || res.Status() != 3 {
		t.Fatalf("exit/signal/status = %d/%d/%d", res.ExitCode, res.Signal, res.Status())
	}
}

func TestRunSignalDeath(t *testing.T) {
	var out, errb bytes.Buffer
	res := testExec().Run(Spec{Seq: 1, Command: "kill -9 $$"}, &out, &errb)
	if !res.Failed() {
		t.Fatal("signal death must count as a failure")
	}
	if res.Signal != int(syscall.SIGKILL) || res.ExitCode != -1 {
		t.Fatalf("signal/exit = %d/%d", res.Signal, res.ExitCode)
	}
	if res.Status() != 137 {
		t.Fatalf("Status = %d, want 137", res.Status())
	}
}

func TestRunKillsProcessGroupOnTimeout(t *testing.T) {
	var out, errb bytes.Buffer
	e := testExec()
	e.Timeout = 200 * time.Millisecond

	start := time.Now()
	res := e.Run(Spec{Seq: 1, Command: "sleep 10"}, &out, &errb)
	dur := time.Since(start)

	if !res.Failed() {
		t.Fatal("timeout must fail the job")
	}
	if !res.TimedOut {
		t.Fatal("TimedOut not set")
	}
	if dur > 3*time.Second {
		t.Fatalf("timeout escalation too slow: %v", dur)
	}
	// A plain sleep dies on the first TERM; the escalation path itself is
	// covered by TestRunEscalatesToKill.
	if res.Signal == 0 {
		t.Fatalf("expected a signal death, got exit %d", res.ExitCode)
	}
	if res.Status() != 128+res.Signal {
		t.Fatalf("Status = %d, want %d", res.Status(), 128+res.Signal)
	}
}

// A job that ignores TERM must still die via the KILL escalation.
func TestRunEscalatesToKill(t *testing.T) {
	var out, errb bytes.Buffer
	e := testExec()
	e.Timeout = 100 * time.Millisecond

	start := time.Now()
	res := e.Run(Spec{Seq: 1, Command: "trap '' TERM; sleep 10"}, &out, &errb)
	if dur := time.Since(start); dur > 3*time.Second {
		t.Fatalf("TERM-ignoring job survived too long: %v", dur)
	}
	if res.Signal != int(syscall.SIGKILL) {
		t.Fatalf("signal = %d, want SIGKILL", res.Signal)
	}
}

func TestRunDryRun(t *testing.T) {
	var out, errb bytes.Buffer
	e := testExec()
	e.DryRun = true

	res := e.Run(Spec{Seq: 1, Command: "echo never-runs"}, &out, &errb)
	if !res.DryRun || res.Failed() {
		t.Fatalf("dry-run result: %+v", res)
	}
	if out.String() != "echo never-runs\n" {
		t.Fatalf("stdout = %q", out.String())
	}
}

func TestRunVerbose(t *testing.T) {
	var out, errb bytes.Buffer
	e := testExec()
	e.Verbose = 1
	e.VerboseW = &errb

	e.Run(Spec{Seq: 1, Command: "true"}, &out, &errb)
	if !strings.Contains(errb.String(), "true") {
		t.Fatalf("verbose did not echo the command: %q", errb.String())
	}
}

func TestRunBadShell(t *testing.T) {
	var out, errb bytes.Buffer
	e := testExec()
	e.Shell = "/nonexistent/sh"

	res := e.Run(Spec{Seq: 1, Command: "true"}, &out, &errb)
	if !res.Failed() || res.ExitCode != 127 {
		t.Fatalf("result = %+v, want failure with exit 127", res)
	}
	if res.Err == nil {
		t.Fatal("the start error must be kept")
	}
}

// A NUL byte makes the argument invalid for exec; the job must fail with the
// error kept so the pool can report it.
func TestRunNULInCommand(t *testing.T) {
	var out, errb bytes.Buffer
	res := testExec().Run(Spec{Seq: 1, Command: "echo a\x00b"}, &out, &errb)
	if !res.Failed() {
		t.Fatal("NUL in command must fail")
	}
	if res.Err == nil {
		t.Fatal("the exec error must be kept")
	}
}

// startIsolated runs sleep in its own process group so tests can signal it
// without touching the test process.
func startIsolated(t *testing.T, command string) (*exec.Cmd, int) {
	t.Helper()
	cmd := exec.Command("/bin/sh", "-c", command)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		cmd.Wait()
	})
	return cmd, cmd.Process.Pid
}

func TestKillGroupTerminatesGroup(t *testing.T) {
	cmd, pgid := startIsolated(t, "sleep 30")
	KillGroup(pgid, []SignalStep{{Signal: syscall.SIGTERM, Wait: 500 * time.Millisecond}})

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("group survived SIGTERM")
	}
	if GroupAlive(pgid) {
		t.Fatal("GroupAlive still true after the group is gone")
	}
}

func TestKillGroupEscalates(t *testing.T) {
	cmd, pgid := startIsolated(t, "trap '' TERM; sleep 30")
	KillGroup(pgid, []SignalStep{
		{Signal: syscall.SIGTERM, Wait: 100 * time.Millisecond},
		{Signal: syscall.SIGKILL, Wait: 500 * time.Millisecond},
	})

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("TERM-ignoring group survived the KILL step")
	}
}

func TestKillGroupIgnoresInvalidPgid(t *testing.T) {
	// Must not signal our own process group.
	KillGroup(0, []SignalStep{{Signal: syscall.SIGTERM, Wait: 0}})
	KillGroup(-1, []SignalStep{{Signal: syscall.SIGTERM, Wait: 0}})
	KillGroup(1, []SignalStep{{Signal: syscall.SIGTERM, Wait: 0}})
}

func TestGroupAliveSelf(t *testing.T) {
	// Our own group is alive.
	if !GroupAlive(syscall.Getpgrp()) {
		t.Fatal("the test's own process group must be alive")
	}
	if GroupAlive(0) || GroupAlive(1) {
		t.Fatal("pgid 0/1 must be reported dead")
	}
}

func TestSignalByName(t *testing.T) {
	if s, err := SignalByName("TERM"); err != nil || s != syscall.SIGTERM {
		t.Fatalf("TERM = %v, %v", s, err)
	}
	if _, err := SignalByName("BOGUS"); err == nil {
		t.Fatal("unknown signal must error")
	}
}

func TestResultStatusAndFailed(t *testing.T) {
	cases := []struct {
		res    Result
		failed bool
		status int
	}{
		{Result{}, false, 0},
		{Result{ExitCode: 0}, false, 0},
		{Result{ExitCode: 5}, true, 5},
		{Result{ExitCode: -1, Signal: 15}, true, 143},
		{Result{Err: errors.New("boom")}, true, 0},
	}
	for _, c := range cases {
		if got := c.res.Failed(); got != c.failed {
			t.Errorf("Failed(%+v) = %v, want %v", c.res, got, c.failed)
		}
		if got := c.res.Status(); got != c.status {
			t.Errorf("Status(%+v) = %d, want %d", c.res, got, c.status)
		}
	}
}
