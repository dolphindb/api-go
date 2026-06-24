package dolphindb

import (
	"errors"
	"testing"
	"time"

	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakePoolConn struct {
	dialer.Conn
	closed  bool
	timeout time.Duration
}

func (f *fakePoolConn) RefreshTimeout(t time.Duration) {
	f.timeout = t
}

func (f *fakePoolConn) Close() error {
	f.closed = true
	return nil
}

func newTestConnPool(size int) *ConnPool {
	return &ConnPool{
		timeout:     time.Second,
		connections: make(chan dialer.Conn, size),
		borrowed:    make(map[dialer.Conn]struct{}, size),
	}
}

func TestConnPoolAcquireReturnsLeaseAndCloseReturnsConnectionOnce(t *testing.T) {
	pool := newTestConnPool(1)
	conn := &fakePoolConn{}
	pool.connections <- conn

	lease, err := pool.Acquire()
	require.NoError(t, err)
	require.Same(t, conn, lease.Conn())
	assert.Equal(t, time.Second, conn.timeout)
	assert.Equal(t, 0, pool.Size())

	require.NoError(t, lease.Close())
	require.NoError(t, lease.Close())
	assert.False(t, conn.closed)
	assert.Equal(t, 1, pool.Size())
}

func TestConnPoolReleaseRejectsConnectionNotBorrowedFromPool(t *testing.T) {
	pool := newTestConnPool(1)
	conn := &fakePoolConn{}

	err := pool.Release(conn)
	require.EqualError(t, err, "connection was not borrowed from this pool")
	assert.False(t, conn.closed)
}

func TestConnPoolReleaseClosesBorrowedConnectionAfterPoolClose(t *testing.T) {
	pool := newTestConnPool(1)
	conn := &fakePoolConn{}
	pool.borrowed[conn] = struct{}{}

	require.NoError(t, pool.Close())
	require.NoError(t, pool.Release(conn))
	assert.True(t, conn.closed)
}

func TestConnPoolWithConnReturnsConnectionWhenCallbackFails(t *testing.T) {
	pool := newTestConnPool(1)
	conn := &fakePoolConn{}
	pool.connections <- conn
	runErr := errors.New("run failed")

	err := pool.WithConn(func(got dialer.Conn) error {
		assert.Same(t, conn, got)
		assert.Equal(t, 0, pool.Size())
		return runErr
	})

	require.ErrorIs(t, err, runErr)
	assert.Equal(t, 1, pool.Size())
	assert.False(t, conn.closed)
}
