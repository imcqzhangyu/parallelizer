package pool

import (
	"errors"
	"strings"
	"testing"

	"parallelizer/internal/cli"
	"parallelizer/internal/job"
)

// JobDone must settle res.Killed in the same critical section that removes the
// job from the running set, so a job's fate is decided exactly once.
func TestJobDoneSettlesKilled(t *testing.T) {
	rule, err := cli.ParseHalt("now,fail=1")
	if err != nil {
		t.Fatal(err)
	}
	st := NewState(rule, nil, 0)
	st.StartJob(1, "sleep 10")
	st.SetPgid(1, 4242)

	// Job 2 fails, which kills running job 1 via --halt now,fail=1.
	st.JobDone(&job.Result{Seq: 2, ExitCode: 1}, false)

	res := job.Result{Seq: 1, ExitCode: -1, Signal: 15}
	if msg := st.JobDone(&res, false); msg != "" {
		t.Fatalf("unexpected report: %q", msg)
	}
	if !res.Killed {
		t.Fatal("job was not marked killed")
	}
	// A killed job is not a failure and must not be counted.
	if got := st.ExitCode(); got != 1 {
		t.Fatalf("exit = %d, want the failing job's status", got)
	}
	if st.WasKilled(1) {
		t.Fatal("killed entry must be cleaned up after JobDone")
	}
}

func TestJobDoneCountsFailures(t *testing.T) {
	st := NewState(cli.HaltRule{When: cli.Never}, nil, 0)
	res := job.Result{Seq: 1, ExitCode: 3}
	st.JobDone(&res, false)
	if res.Killed {
		t.Fatal("job must not be killed")
	}
	if got := st.ExitCode(); got != 1 {
		t.Fatalf("exit = %d, want 1 failed job", got)
	}
}

// A job that fails internally (e.g. its output could not be spooled) must be
// reported even when no --halt rule is set, otherwise the failure is silent.
func TestJobDoneReportsInternalError(t *testing.T) {
	st := NewState(cli.HaltRule{When: cli.Never}, nil, 0)
	res := job.Result{Seq: 1, ExitCode: 1, Err: errors.New("cannot spill output to /x: no space")}
	msg := st.JobDone(&res, false)
	if !strings.Contains(msg, "cannot spill output") {
		t.Fatalf("report = %q, want the cause included", msg)
	}
	if quiet := st.JobDone(&job.Result{Seq: 2, ExitCode: 1, Err: errors.New("boom")}, true); quiet != "" {
		t.Fatalf("--quiet still reported: %q", quiet)
	}
}

func TestInterruptKillsRunningAndForcesExit(t *testing.T) {
	st := NewState(cli.HaltRule{When: cli.Never}, nil, 0)
	st.SetPgid(1, 4242)
	st.SetPgid(2, 4343)

	st.Interrupt(130)
	if got := st.ExitCode(); got != 130 {
		t.Fatalf("exit = %d, want 130", got)
	}
	if st.StartJob(3, "true") {
		t.Fatal("new jobs must be rejected after an interrupt")
	}
	for _, seq := range []int{1, 2} {
		res := job.Result{Seq: seq, ExitCode: -1, Signal: 15}
		st.JobDone(&res, false)
		if !res.Killed {
			t.Fatalf("job %d was running at interrupt and must be marked killed", seq)
		}
	}
}

// --halt now,fail=1 kills everything still running and reports the trigger.
func TestHaltNowFailsAndKills(t *testing.T) {
	rule, err := cli.ParseHalt("now,fail=1")
	if err != nil {
		t.Fatal(err)
	}
	st := NewState(rule, nil, 0)
	st.StartJob(1, "sleep 10")
	st.SetPgid(1, 4242)

	res := job.Result{Seq: 2, ExitCode: 7}
	st.JobDone(&res, false)
	if got := st.ExitCode(); got != 7 {
		t.Fatalf("exit = %d, want the failing job's status 7", got)
	}
	res1 := job.Result{Seq: 1, ExitCode: -1, Signal: 15}
	st.JobDone(&res1, false)
	if !res1.Killed {
		t.Fatal("running job must be killed by --halt now,fail=1")
	}
	if st.StartJob(3, "true") {
		t.Fatal("no new jobs may start after the halt")
	}
}

// A resumed (--joblog) job must not be counted at all, and percentage halt
// rules must see the true total.
func TestHaltSoonDonePercentage(t *testing.T) {
	rule, err := cli.ParseHalt("soon,done=50%")
	if err != nil {
		t.Fatal(err)
	}
	st := NewState(rule, nil, 4)
	for seq := 1; seq <= 2; seq++ {
		st.StartJob(seq, "true")
		res := job.Result{Seq: seq, ExitCode: 0}
		st.JobDone(&res, false)
	}
	if st.ExitCode() != 0 {
		t.Fatalf("exit = %d, want 0", st.ExitCode())
	}
	if st.StartJob(3, "true") {
		t.Fatal("50%% of 4 jobs done: no further jobs may start")
	}
}
