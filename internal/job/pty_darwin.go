//go:build darwin

package job

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"syscall"
	"unsafe"
)

// openPty opens /dev/ptmx, names the slave with TIOCPTYGNAME, then applies
// Apple's grantpt()/unlockpt() sequence (TIOCPTYGRANT + TIOCPTYUNLK).
// The slave name comes back as a full /dev/... path.
func openPty() (*os.File, *os.File, error) {
	m, err := os.OpenFile("/dev/ptmx", os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("open /dev/ptmx: %w", err)
	}
	var buf [128]byte
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, m.Fd(),
		uintptr(syscall.TIOCPTYGNAME), uintptr(unsafe.Pointer(&buf[0]))); errno != 0 {
		m.Close()
		return nil, nil, fmt.Errorf("pty name: %w", errno)
	}
	name := string(bytes.TrimRight(buf[:], "\x00"))
	// Apple's grantpt()/unlockpt() in one shot: TIOCPTYGRANT hands the fresh
	// slave (root:tty 0620) to the opener, TIOCPTYUNLK drops the lock the
	// kernel puts on a freshly created pair (open then returns EAGAIN).
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, m.Fd(),
		uintptr(syscall.TIOCPTYGRANT), 0); errno != 0 {
		m.Close()
		return nil, nil, fmt.Errorf("grant pty: %w", errno)
	}
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, m.Fd(),
		uintptr(syscall.TIOCPTYUNLK), 0); errno != 0 {
		m.Close()
		return nil, nil, fmt.Errorf("unlock pty: %w", errno)
	}
	path := name
	if !strings.HasPrefix(path, "/") {
		path = "/dev/" + path // XNU returns a full path; Linux-style names get the prefix
	}
	s, err := os.OpenFile(path, os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		m.Close()
		return nil, nil, fmt.Errorf("open pty slave %s: %w", path, err)
	}
	return m, s, nil
}
