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
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, m.Fd(),
		uintptr(syscall.TIOCSPTLCK), uintptr(unsafe.Pointer(&unlock))); errno != 0 {
		m.Close()
		return nil, nil, fmt.Errorf("unlock pty: %w", errno)
	}
	var n uint32
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, m.Fd(),
		uintptr(syscall.TIOCGPTN), uintptr(unsafe.Pointer(&n))); errno != 0 {
		m.Close()
		return nil, nil, fmt.Errorf("pty number: %w", errno)
	}
	s, err := os.OpenFile(fmt.Sprintf("/dev/pts/%d", n), os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		m.Close()
		return nil, nil, fmt.Errorf("open pty slave: %w", err)
	}
	return m, s, nil
}
