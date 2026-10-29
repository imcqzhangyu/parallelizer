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

// TerminalSize reports the window size of fd; ok is false when fd is not a
// terminal.
func TerminalSize(fd uintptr) (winsize, bool) {
	var ws winsize
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd,
		uintptr(syscall.TIOCGWINSZ), uintptr(unsafe.Pointer(&ws)))
	return ws, errno == 0
}

func setTerminalSize(fd uintptr, ws winsize) error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd,
		uintptr(syscall.TIOCSWINSZ), uintptr(unsafe.Pointer(&ws)))
	if errno != 0 {
		return errno
	}
	return nil
}

// OpenPty allocates a pseudo-terminal pair. The slave becomes the job's
// stdin/stdout/stderr together with Setsid+Setctty (job's controlling
// terminal); the caller closes both ends.
func OpenPty() (master, slave *os.File, err error) {
	return openPty()
}
