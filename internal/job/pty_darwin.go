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
	if err := ioctl(m, syscall.TIOCPTYGNAME, unsafe.Pointer(&buf[0])); err != nil {
		m.Close()
		return nil, nil, fmt.Errorf("pty name: %w", err)
	}
	name := string(bytes.TrimRight(buf[:], "\x00"))
	// Apple's grantpt()/unlockpt(): TIOCPTYGRANT hands the fresh slave
	// (root:tty 0620) to the opener, TIOCPTYUNLK drops the lock the kernel
	// puts on a freshly created pair (open then returns EAGAIN).
	if err := ioctl(m, syscall.TIOCPTYGRANT, nil); err != nil {
		m.Close()
		return nil, nil, fmt.Errorf("grant pty: %w", err)
	}
	if err := ioctl(m, syscall.TIOCPTYUNLK, nil); err != nil {
		m.Close()
		return nil, nil, fmt.Errorf("unlock pty: %w", err)
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
	if err := makeBlocking(s); err != nil {
		s.Close()
		m.Close()
		return nil, nil, fmt.Errorf("prepare pty slave: %w", err)
	}
	return m, s, nil
}
