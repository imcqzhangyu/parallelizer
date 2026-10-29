package job

import (
	"fmt"
	"syscall"
	"time"
)

// SignalStep mirrors cli.SignalStep so this package stays dependency-free.
type SignalStep struct {
	Signal syscall.Signal
	Wait   time.Duration
}

var nameToSignal = map[string]syscall.Signal{
	"HUP":    syscall.SIGHUP,
	"INT":    syscall.SIGINT,
	"QUIT":   syscall.SIGQUIT,
	"ILL":    syscall.SIGILL,
	"TRAP":   syscall.SIGTRAP,
	"ABRT":   syscall.SIGABRT,
	"BUS":    syscall.SIGBUS,
	"FPE":    syscall.SIGFPE,
	"KILL":   syscall.SIGKILL,
	"USR1":   syscall.SIGUSR1,
	"SEGV":   syscall.SIGSEGV,
	"USR2":   syscall.SIGUSR2,
	"PIPE":   syscall.SIGPIPE,
	"ALRM":   syscall.SIGALRM,
	"TERM":   syscall.SIGTERM,
	"CHLD":   syscall.SIGCHLD,
	"CONT":   syscall.SIGCONT,
	"STOP":   syscall.SIGSTOP,
	"TSTP":   syscall.SIGTSTP,
	"TTIN":   syscall.SIGTTIN,
	"TTOU":   syscall.SIGTTOU,
	"URG":    syscall.SIGURG,
	"XCPU":   syscall.SIGXCPU,
	"XFSZ":   syscall.SIGXFSZ,
	"VTALRM": syscall.SIGVTALRM,
	"PROF":   syscall.SIGPROF,
	"WINCH":  syscall.SIGWINCH,
	"SYS":    syscall.SIGSYS,
	"IO":     syscall.SIGIO,
}

// SignalByName resolves a signal name such as "TERM".
func SignalByName(name string) (syscall.Signal, error) {
	if s, ok := nameToSignal[name]; ok {
		return s, nil
	}
	return 0, fmt.Errorf("unknown signal %q", name)
}

// KillGroup signals the whole process group and returns once it is gone or the
// step's grace period expires.
//
// The group is checked for existence immediately before every signal: the
// caller may have raced with the job being reaped (its pgid then becomes
// eligible for reuse by an unrelated process).
func KillGroup(pgid int, steps []SignalStep) {
	if pgid <= 1 {
		return
	}
	for _, st := range steps {
		if syscall.Kill(-pgid, 0) != nil {
			return // ESRCH: the whole group is gone
		}
		_ = syscall.Kill(-pgid, st.Signal)
		if st.Wait <= 0 {
			continue
		}
		deadline := time.Now().Add(st.Wait)
		for time.Now().Before(deadline) {
			if err := syscall.Kill(-pgid, 0); err != nil {
				return // ESRCH: the whole group is gone
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// GroupAlive reports whether any process in the group still exists.
func GroupAlive(pgid int) bool {
	return pgid > 1 && syscall.Kill(-pgid, 0) == nil
}
