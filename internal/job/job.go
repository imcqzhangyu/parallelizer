package job

import (
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync/atomic"
	"syscall"
	"time"
)

// Exec runs a single shell command.
type Exec struct {
	Shell    string
	Timeout  time.Duration
	TermSeq  []SignalStep
	DryRun   bool
	Verbose  int
	VerboseW io.Writer
	// OnStart is called with the job's process group id as soon as it exists.
	OnStart func(seq, pgid int)
}

// Run executes spec and returns its result. Output goes to stdout and stderr.
func (e *Exec) Run(spec Spec, stdout, stderr io.Writer) Result {
	res := Result{Seq: spec.Seq, Start: time.Now()}

	if e.Verbose > 0 && e.VerboseW != nil {
		fmt.Fprintln(e.VerboseW, spec.Command)
	}
	if e.DryRun {
		fmt.Fprintln(stdout, spec.Command)
		res.DryRun = true
		res.Runtime = time.Since(res.Start)
		return res
	}

	cmd := exec.Command(e.Shell, "-c", spec.Command)
	cmd.Stdin = nil // /dev/null
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		res.Err = err
		res.ExitCode = 127
		res.Runtime = time.Since(res.Start)
		return res
	}

	pgid := cmd.Process.Pid
	if e.OnStart != nil {
		e.OnStart(spec.Seq, pgid)
	}

	var timedOut atomic.Bool
	var timer *time.Timer
	if e.Timeout > 0 {
		timer = time.AfterFunc(e.Timeout, func() {
			timedOut.Store(true)
			KillGroup(pgid, e.TermSeq)
		})
	}

	err := cmd.Wait()
	if timer != nil {
		timer.Stop()
	}

	res.Runtime = time.Since(res.Start)
	res.TimedOut = timedOut.Load()
	res.finish(cmd, err)
	return res
}

func (r *Result) finish(cmd *exec.Cmd, err error) {
	if cmd.ProcessState == nil {
		r.Err = err
		r.ExitCode = 127
		return
	}
	if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		r.Signal = int(ws.Signal())
		r.ExitCode = -1
	} else {
		r.ExitCode = cmd.ProcessState.ExitCode()
	}
	var ee *exec.ExitError
	if err != nil && !errors.As(err, &ee) {
		r.Err = err
		if r.ExitCode == 0 && r.Signal == 0 {
			r.ExitCode = 127
		}
	}
}
