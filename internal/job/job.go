package job

import (
	"errors"
	"fmt"
	"io"
	"os"
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

	// Pty runs the job on its own pseudo-terminal (--pty): the job gets a
	// controlling terminal (so TUIs can draw and read input), stdin is the
	// terminal instead of /dev/null, and stdout/stderr are merged — they all
	// arrive together on the job's stdout writer. Tty forwards our terminal's
	// keystrokes into the pty and may be nil (no interactivity).
	Pty bool
	Tty *TtyHub
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
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	// Give every job its own session (and thus its own process group: a
	// session leader's pgid is its pid, so KillGroup's kill(-pgid) semantics
	// are unchanged). The session has no controlling terminal, so a job that
	// opens /dev/tty (a TUI waiting for input) fails immediately with ENXIO
	// instead of blocking forever on a SIGTTIN stop and leaving the user's
	// terminal in a raw-mode state we never set. TUIs that really need a
	// terminal are served by --pty instead.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	var master, slave *os.File
	if e.Pty {
		m, s, err := OpenPty()
		if err != nil {
			res.Err = err
			res.ExitCode = 127
			res.Runtime = time.Since(res.Start)
			return res
		}
		master, slave = m, s
		if ws, ok := e.Tty.WindowSize(); ok && ws.Row > 0 && ws.Col > 0 {
			setTerminalSize(slave.Fd(), ws) // best effort: TUIs need a sane size
		}
		cmd.Stdin = slave // a terminal, not /dev/null
		cmd.Stdout = slave
		cmd.Stderr = slave
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true, Ctty: 0}
	} else {
		cmd.Stdin = nil // /dev/null
	}

	if err := cmd.Start(); err != nil {
		if slave != nil {
			slave.Close()
		}
		if master != nil {
			master.Close()
		}
		res.Err = err
		res.ExitCode = 127
		res.Runtime = time.Since(res.Start)
		return res
	}
	if slave != nil {
		slave.Close() // the child holds its own references now
	}

	// --pty: the child's merged stdout/stderr stream out of the pty into the
	// job's normal output writer (sinks, tags, keep-order all downstream).
	// Keystrokes flow the other way through the hub.
	var copyDone chan struct{}
	if master != nil {
		e.Tty.Add(master) // nil-safe: registers nothing without a hub
		copyDone = make(chan struct{})
		go func() {
			defer close(copyDone)
			_, _ = io.Copy(stdout, master)
		}()
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
	if master != nil {
		e.Tty.Remove(master)
		// Normally the master sees EOF/EIO as soon as the last slave fd is
		// gone. A grandchild can keep it open: close ours after a grace
		// period so the copy goroutine cannot outlive the job forever.
		select {
		case <-copyDone:
		case <-time.After(500 * time.Millisecond):
			master.Close()
			<-copyDone
		}
		master.Close()
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
