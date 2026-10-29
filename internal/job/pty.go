package job

// Pseudo-terminal support for --pty. The platform-specific openPty lives in
// pty_linux.go / pty_darwin.go; everything here is shared by both.

import (
	"os"
	"syscall"
	"unsafe"
)

// winsize mirrors struct winsize (all supported platforms).
type winsize struct {
	Row    uint16
	Col    uint16
	Xpixel uint16
	Ypixel uint16
}

// ioctl performs req on f without touching its blocking mode. Doing the ioctl
// through SyscallConn (instead of f.Fd()) matters: File.Fd() switches the
// descriptor back to blocking mode, which would make a later Read/Write
// uninterruptible by File.Close and turn the --pty shutdown path into a hang.
func ioctl(f *os.File, req uintptr, arg unsafe.Pointer) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var errno syscall.Errno
	if cerr := rc.Control(func(fd uintptr) {
		_, _, errno = syscall.Syscall(syscall.SYS_IOCTL, fd, req, uintptr(arg))
	}); cerr != nil {
		return cerr
	}
	if errno != 0 {
		return errno
	}
	return nil
}

// terminalSize reports the window size of f; ok is false when f is not a
// terminal.
func terminalSize(f *os.File) (winsize, bool) {
	var ws winsize
	if err := ioctl(f, syscall.TIOCGWINSZ, unsafe.Pointer(&ws)); err != nil {
		return winsize{}, false
	}
	return ws, true
}

func setTerminalSize(f *os.File, ws winsize) error {
	return ioctl(f, syscall.TIOCSWINSZ, unsafe.Pointer(&ws))
}

// makeBlocking puts the slave into blocking mode. The child inherits the open
// file description, and an O_NONBLOCK stdin/stdout/stderr would make every
// read/write in the job fail with EAGAIN.
func makeBlocking(f *os.File) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var setErr error
	if cerr := rc.Control(func(fd uintptr) {
		setErr = syscall.SetNonblock(int(fd), false)
	}); cerr != nil {
		return cerr
	}
	return setErr
}

// OpenPty allocates a pseudo-terminal pair. The slave becomes the job's
// stdin/stdout/stderr together with Setsid+Setctty (job's controlling
// terminal); the caller closes both ends. The master stays non-blocking and
// pollable so Close can interrupt a pending read.
func OpenPty() (master, slave *os.File, err error) {
	return openPty()
}
