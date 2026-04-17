package dialer

import (
	"errors"
	"fmt"
	"net"
	"syscall"
	"time"
)

const (
	defaultConnectTimeout    = 3 * time.Second
	defaultKeepAliveTime     = 30 * time.Second
	defaultTCPUserTimeout    = 15 * time.Second
	defaultKeepAliveProbeCnt = 3
)

type tcpSocketOptions struct {
	keepAliveTime     time.Duration
	keepAliveInterval time.Duration
	keepAliveCount    int
	tcpUserTimeout    time.Duration
}

func (c *conn) connectTimeout() time.Duration {
	if c.behaviorOpt == nil || c.behaviorOpt.NetTimeout == 0 {
		return defaultConnectTimeout
	}

	return c.behaviorOpt.NetTimeout
}

func (c *conn) tcpSocketOptions() tcpSocketOptions {
	opt := tcpSocketOptions{
		keepAliveTime:  defaultKeepAliveTime,
		tcpUserTimeout: defaultTCPUserTimeout,
	}
	if c.behaviorOpt == nil {
		opt.keepAliveInterval = defaultTCPUserTimeout / defaultKeepAliveProbeCnt
		opt.keepAliveCount = defaultKeepAliveProbeCnt
		return opt
	}

	if c.behaviorOpt.NetTimeout > 0 {
		opt.keepAliveTime = c.behaviorOpt.NetTimeout
		opt.tcpUserTimeout = c.behaviorOpt.NetTimeout
		opt.keepAliveInterval = c.behaviorOpt.NetTimeout / defaultKeepAliveProbeCnt
		if opt.keepAliveInterval < time.Second {
			opt.keepAliveInterval = time.Second
		}
		opt.keepAliveCount = defaultKeepAliveProbeCnt
	}

	return opt
}

func wrapNetTimeoutSocketOptionError(err error, opt tcpSocketOptions) error {
	if err == nil {
		return err
	}
	if errors.Is(err, syscall.EINVAL) {
		return fmt.Errorf("NetTimeout too long (%s): %w", opt.keepAliveTime, err)
	}
	return err
}

func setTCPSocketOptions(conn *net.TCPConn, opt tcpSocketOptions) error {
	if opt.keepAliveTime < 0 {
		if err := conn.SetKeepAlive(false); err != nil {
			return wrapNetTimeoutSocketOptionError(err, opt)
		}
		return setPlatformTCPSocketOptions(conn, opt)
	}

	if err := conn.SetKeepAlive(true); err != nil {
		return wrapNetTimeoutSocketOptionError(err, opt)
	}
	if err := conn.SetKeepAlivePeriod(opt.keepAliveTime); err != nil {
		return wrapNetTimeoutSocketOptionError(err, opt)
	}

	return setPlatformTCPSocketOptions(conn, opt)
}
