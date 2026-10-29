package job

import "time"

// Spec is one line of the command file.
type Spec struct {
	Seq     int
	Command string
}

// Result describes how a job ended.
type Result struct {
	Seq      int
	ExitCode int // -1 when the job died from a signal
	Signal   int // 0 when the job exited normally
	Start    time.Time
	Runtime  time.Duration
	Received int64 // bytes written to stdout+stderr
	TimedOut bool
	Killed   bool // killed because of --halt
	DryRun   bool
	Err      error
}

// Failed reports whether the job counts as a failure.
func (r Result) Failed() bool {
	if r.Err != nil {
		return true
	}
	if r.Signal != 0 {
		return true
	}
	return r.ExitCode != 0
}

// Status returns the shell-visible status (128+signal for signal deaths).
func (r Result) Status() int {
	if r.Signal != 0 {
		return 128 + r.Signal
	}
	return r.ExitCode
}
