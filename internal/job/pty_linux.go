//go:build linux

package job

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// openPty opens /dev/ptmx, unlocks the pair (TIOCSPTLCK) and resolves the
// slave through TIOCGPTN (/dev/pts/N).
func openPty() (*os.File, *os.File, error) {
	m, err := os.OpenFile("/dev/ptmx", os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("open /dev/ptmx: %w", err)
	}
	var unlock int32 // 0 = unlock
	if err := ioctl(m, syscall.TIOCSPTLCK, unsafe.Pointer(&unlock)); err != nil {
		m.Close()
		return nil, nil, fmt.Errorf("unlock pty: %w", err)
	}
	var n uint32
	if err := ioctl(m, syscall.TIOCGPTN, unsafe.Pointer(&n)); err != nil {
		m.Close()
		return nil, nil, fmt.Errorf("pty number: %w", err)
	}
	s, err := os.OpenFile(fmt.Sprintf("/dev/pts/%d", n), os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		m.Close()
		return nil, nil, fmt.Errorf("open pty slave: %w", err)
	}
	if err := makeBlocking(s); err != nil {
		s.Close()
		m.Close()
		return nil, nil, fmt.Errorf("prepare pty slave: %w", err)
	}
	return m, s, nil
}
