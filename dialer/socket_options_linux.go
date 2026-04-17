//go:build linux
// +build linux

package dialer

import (
	"net"
	"syscall"
	"time"
)

const linuxTCPUserTimeout = 0x12

func setPlatformTCPSocketOptions(conn *net.TCPConn, opt tcpSocketOptions) error {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return err
	}

	var socketErr error
	err = rawConn.Control(func(fd uintptr) {
		if opt.keepAliveTime >= 0 && opt.keepAliveInterval > 0 {
			secs := durationToSeconds(opt.keepAliveInterval)
			socketErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, secs)
			if socketErr != nil {
				return
			}
		}
		if opt.keepAliveTime >= 0 && opt.keepAliveCount > 0 {
			socketErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, opt.keepAliveCount)
			if socketErr != nil {
				return
			}
		}
		if opt.tcpUserTimeout > 0 {
			ms := durationToMilliseconds(opt.tcpUserTimeout)
			socketErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, linuxTCPUserTimeout, ms)
		}
	})
	if err != nil {
		return err
	}

	return wrapNetTimeoutSocketOptionError(socketErr, opt)
}

func durationToSeconds(d time.Duration) int {
	if d <= 0 {
		return 1
	}

	secs := int(d / time.Second)
	if d%time.Second != 0 {
		secs++
	}
	if secs < 1 {
		return 1
	}

	return secs
}

func durationToMilliseconds(d time.Duration) int {
	if d <= 0 {
		return 1
	}

	ms := int(d / time.Millisecond)
	if d%time.Millisecond != 0 {
		ms++
	}
	if ms < 1 {
		return 1
	}

	return ms
}
