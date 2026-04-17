package dialer

import (
	"errors"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWrapNetTimeoutSocketOptionErrorAnnotatesEINVAL(t *testing.T) {
	err := wrapNetTimeoutSocketOptionError(syscall.EINVAL, tcpSocketOptions{
		tcpUserTimeout: 24 * time.Hour,
	})

	assert.Error(t, err)
	assert.True(t, errors.Is(err, syscall.EINVAL))
	assert.Contains(t, err.Error(), "NetTimeout too long")
	assert.Contains(t, err.Error(), "24h0m0s")
}

func TestWrapNetTimeoutSocketOptionErrorPassesThroughOtherErrors(t *testing.T) {
	baseErr := errors.New("setsockopt: invalid argument")
	err := wrapNetTimeoutSocketOptionError(baseErr, tcpSocketOptions{
		tcpUserTimeout: 24 * time.Hour,
	})

	assert.Same(t, baseErr, err)
	assert.False(t, strings.Contains(err.Error(), "NetTimeout too long"))
}

func TestWrapNetTimeoutSocketOptionErrorAnnotatesNestedEINVAL(t *testing.T) {
	baseErr := &net.OpError{
		Op:  "set",
		Net: "tcp",
		Err: &os.SyscallError{
			Syscall: "setsockopt",
			Err:     syscall.EINVAL,
		},
	}

	err := wrapNetTimeoutSocketOptionError(baseErr, tcpSocketOptions{
		keepAliveTime: 30 * 24 * time.Hour,
	})

	assert.Error(t, err)
	assert.True(t, errors.Is(err, syscall.EINVAL))
	assert.Contains(t, err.Error(), "NetTimeout too long")
	assert.Contains(t, err.Error(), "720h0m0s")
}
