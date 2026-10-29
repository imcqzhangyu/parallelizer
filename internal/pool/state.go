package pool

import (
	"fmt"
	"io"
	"sort"
	"sync"

	"parallelizer/internal/cli"
	"parallelizer/internal/job"
)

// State tracks running jobs and decides when to halt.
type State struct {
	mu    sync.Mutex
	rule  cli.HaltRule
	steps []job.SignalStep
	total int

	running map[int]int    // seq -> pgid
	cmds    map[int]string // seq -> command
	killed  map[int]bool   // seq -> killed by --halt

	failed    int
	succeeded int
	finished  int

	halting  bool
	halted   chan struct{}
	exitCode int
	exitSet  bool

	// killOnStart says a job that registers its pgid after halting began must
	// be killed immediately. True for `--halt now,...` (running jobs die) and
	// for a signal; false for `soon`, which lets started jobs finish.
	killOnStart bool
}

func NewState(rule cli.HaltRule, steps []job.SignalStep, total int) *State {
	return &State{
		rule:    rule,
		steps:   steps,
		total:   total,
		running: make(map[int]int),
		cmds:    make(map[int]string),
		killed:  make(map[int]bool),
		halted:  make(chan struct{}),
	}
}

// Halted is closed the moment the scheduler stops accepting new jobs.
func (s *State) Halted() <-chan struct{} { return s.halted }

// IsHalted reports whether halting has begun (non-blocking).
func (s *State) IsHalted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.halting
}

// StartJob reserves a slot. It returns false once halting has begun.
func (s *State) StartJob(seq int, cmd string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.halting {
		return false
	}
	s.cmds[seq] = cmd
	return true
}

// SetPgid registers the process group. If halting started in the meantime the
// job is either killed right away (now rules, signals) or allowed to finish
// (soon rules), closing the start/kill race.
func (s *State) SetPgid(seq, pgid int) {
	s.mu.Lock()
	if s.halting {
		if !s.killOnStart {
			// soon: the job was admitted before the halt, let it finish.
			s.running[seq] = pgid
			s.mu.Unlock()
			return
		}
		s.killed[seq] = true
		s.mu.Unlock()
		go job.KillGroup(pgid, s.steps)
		return
	}
	s.running[seq] = pgid
	s.mu.Unlock()
}

// Reaped drops a pgid as soon as its process has been reaped, so a concurrent
// halt cannot signal a process group the kernel may have recycled.
func (s *State) Reaped(seq int) {
	s.mu.Lock()
	delete(s.running, seq)
	s.mu.Unlock()
}

// WasKilled reports whether the job was terminated by our own halt logic.
func (s *State) WasKilled(seq int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.killed[seq]
}

// JobDone accounts for a finished job and may fire the halt rule. It settles
// res.Killed and returns a human-readable message to print after the job's
// output ("" when there is nothing to report).
//
// Settling res.Killed here, in the same critical section that removes the job
// from s.running, closes the window in which a job that has already exited
// could still be marked killed (or its, possibly reused, process group
// signalled) by a concurrent halt.
func (s *State) JobDone(res *job.Result, quiet bool) string {
	s.mu.Lock()

	delete(s.running, res.Seq)
	killed := s.killed[res.Seq]
	delete(s.killed, res.Seq)
	res.Killed = killed
	cmd := s.cmds[res.Seq]
	delete(s.cmds, res.Seq)

	s.finished++
	if !killed {
		if res.Failed() {
			s.failed++
		} else {
			s.succeeded++
		}
	}

	report := ""
	if !killed && s.rule.Set && !quiet {
		switch {
		case s.rule.On == cli.OnFail && res.Failed():
			report = "job failed"
		case s.rule.On == cli.OnSuccess && !res.Failed():
			report = "job succeeded"
		}
	}
	// A job can fail without any output or halt rule active (e.g. it could not
	// be started, or output could not be spooled to disk). Such failures must
	// still reach the user.
	if res.Failed() && !killed && res.Err != nil && !quiet && report == "" {
		report = "job failed"
	}

	var toKill []int
	if !s.halting && s.rule.ShouldHalt(s.failed, s.succeeded, s.finished, s.total) {
		s.halting = true
		close(s.halted)
		s.exitCode = s.rule.ExitCode(s.failed, s.total, res.ExitCode, res.Signal)
		s.exitSet = true
		if s.rule.When == cli.Now {
			s.killOnStart = true
			for seq, pgid := range s.running {
				s.killed[seq] = true
				toKill = append(toKill, pgid)
			}
		}
	}

	s.mu.Unlock()

	for _, pgid := range toKill {
		go job.KillGroup(pgid, s.steps)
	}
	if report == "" {
		return ""
	}
	if res.Err != nil {
		return fmt.Sprintf("parallelizer: %s (%v):\n%s\n", report, res.Err, cmd)
	}
	return fmt.Sprintf("parallelizer: %s:\n%s\n", report, cmd)
}

// Interrupt stops everything, e.g. on SIGINT. A signal wins over a halt rule:
// the user asked to stop now, so its exit status replaces the rule's.
func (s *State) Interrupt(code int) {
	s.mu.Lock()
	if !s.halting {
		s.halting = true
		close(s.halted)
	}
	s.exitCode = code
	s.exitSet = true
	s.killOnStart = true
	var toKill []int
	for seq, pgid := range s.running {
		s.killed[seq] = true
		toKill = append(toKill, pgid)
	}
	s.mu.Unlock()
	for _, pgid := range toKill {
		go job.KillGroup(pgid, s.steps)
	}
}

// ExitCode is the value main should exit with.
func (s *State) ExitCode() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.exitSet {
		return s.exitCode
	}
	return s.rule.FinalExitCode(s.failed, s.total)
}

// DumpRunning prints the jobs that are still alive (SIGUSR1).
func (s *State) DumpRunning(w io.Writer) {
	s.mu.Lock()
	type row struct {
		seq  int
		cmd  string
		pgid int
	}
	rows := make([]row, 0, len(s.running))
	for seq, pgid := range s.running {
		rows = append(rows, row{seq: seq, pgid: pgid, cmd: s.cmds[seq]})
	}
	s.mu.Unlock()

	sort.Slice(rows, func(i, j int) bool { return rows[i].seq < rows[j].seq })
	fmt.Fprintf(w, "parallelizer: %d job(s) running\n", len(rows))
	for _, r := range rows {
		fmt.Fprintf(w, "  #%d (pgid %d): %s\n", r.seq, r.pgid, r.cmd)
	}
}
