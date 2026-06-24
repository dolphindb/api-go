package dialer

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/dolphindb/api-go/v3/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunScriptWithTraceReturnsEmptyTraceWithoutFailover(t *testing.T) {
	originalRunInternal := runInternalRequest
	defer func() {
		runInternalRequest = originalRunInternal
	}()

	runInternalRequest = func(c *conn, params *requestParams) (*responseHeader, model.DataForm, error) {
		return nil, nil, nil
	}

	rawConn, err := NewConn(context.TODO(), "127.0.0.1:8848", nil)
	require.NoError(t, err)

	_, trace, err := rawConn.RunScriptWithTrace("1+1")
	require.NoError(t, err)
	require.NotNil(t, trace)
	assert.Empty(t, trace.Failovers)
}

func TestRunScriptWithTraceRecordsNotLeaderFailover(t *testing.T) {
	originalRunInternal := runInternalRequest
	originalConnectToAddress := connectToAddress
	defer func() {
		runInternalRequest = originalRunInternal
		connectToAddress = originalConnectToAddress
	}()

	runCount := 0
	runInternalRequest = func(c *conn, params *requestParams) (*responseHeader, model.DataForm, error) {
		runCount++
		if runCount == 1 {
			return nil, nil, newServerError("<NotLeader>10.0.0.2:8848:dnode2")
		}
		return nil, nil, nil
	}

	var connectedTo string
	connectToAddress = func(c *conn, addr string) error {
		connectedTo = addr
		return nil
	}

	rawConn, err := NewConn(context.TODO(), "10.0.0.1:8848", nil)
	require.NoError(t, err)
	c := rawConn.(*conn)
	c.isConnected = true
	c.nodePool = newNodePool("10.0.0.1:8848", nil)

	_, trace, err := c.RunScriptWithTrace("tableInsert(t, 1)")
	require.NoError(t, err)
	require.NotNil(t, trace)
	require.Len(t, trace.Failovers, 1)
	assert.Equal(t, FailoverReasonNotLeader, trace.Failovers[0].Reason)
	assert.Equal(t, "10.0.0.1:8848", trace.Failovers[0].From)
	assert.Equal(t, "10.0.0.2:8848", trace.Failovers[0].To)
	assert.Equal(t, "client error response. <NotLeader>10.0.0.2:8848:dnode2", trace.Failovers[0].Err)
	assert.Equal(t, "10.0.0.2:8848", connectedTo)
	assert.Equal(t, 2, runCount)
}

func TestRunFuncWithTraceStopsWhenNotLeaderTargetRepeats(t *testing.T) {
	originalRunInternal := runInternalRequest
	originalConnectToAddress := connectToAddress
	defer func() {
		runInternalRequest = originalRunInternal
		connectToAddress = originalConnectToAddress
	}()

	runCount := 0
	runInternalRequest = func(c *conn, params *requestParams) (*responseHeader, model.DataForm, error) {
		runCount++
		return nil, nil, newServerError("<NotLeader>10.0.0.2:8848:dnode2")
	}

	connectCount := 0
	connectToAddress = func(c *conn, addr string) error {
		connectCount++
		c.sessionID = []byte("new-session")
		return nil
	}

	rawConn, err := NewConn(context.TODO(), "10.0.0.1:8848", &BehaviorOptions{Reconnect: true})
	require.NoError(t, err)
	c := rawConn.(*conn)
	c.isConnected = true
	c.sessionID = []byte("old-session")
	c.nodePool = newNodePool("10.0.0.1:8848", nil)

	_, trace, err := c.RunFuncWithTrace("tableInsert{t}", nil)
	require.Error(t, err)
	require.NotNil(t, trace)
	require.Len(t, trace.Failovers, 1)
	assert.Equal(t, "10.0.0.2:8848", trace.Failovers[0].To)
	assert.Equal(t, 2, runCount)
	assert.Equal(t, 1, connectCount)
}

func TestRunScriptWithTraceRecordsMappedNotLeaderFailover(t *testing.T) {
	originalRunInternal := runInternalRequest
	originalConnectToAddress := connectToAddress
	defer func() {
		runInternalRequest = originalRunInternal
		connectToAddress = originalConnectToAddress
	}()

	runCount := 0
	runInternalRequest = func(c *conn, params *requestParams) (*responseHeader, model.DataForm, error) {
		runCount++
		if runCount == 1 {
			return nil, nil, newServerError("<NotLeader>10.0.0.2:8848:dnode2")
		}
		return nil, nil, nil
	}

	var connectedTo string
	connectToAddress = func(c *conn, addr string) error {
		connectedTo = addr
		return nil
	}

	rawConn, err := NewConn(context.TODO(), "10.0.0.1:8848", nil)
	require.NoError(t, err)
	c := rawConn.(*conn)
	c.isConnected = true
	c.nodePool = newNodePool("10.0.0.1:8848", nil)
	c.isPublicName = true
	c.publicNameByAddress = map[string]string{
		"10.0.0.2:8848": "public.example.com:8848",
	}
	c.addressByPublicName = map[string]string{
		"public-1.example.com:8848": "10.0.0.1:8848",
		"public.example.com:8848":   "10.0.0.2:8848",
	}

	_, trace, err := c.RunScriptWithTrace("tableInsert(t, 1)")
	require.NoError(t, err)
	require.NotNil(t, trace)
	require.Len(t, trace.Failovers, 1)
	assert.Equal(t, FailoverReasonNotLeader, trace.Failovers[0].Reason)
	assert.Equal(t, "10.0.0.1:8848", trace.Failovers[0].From)
	assert.Equal(t, "public.example.com:8848", trace.Failovers[0].To)
	assert.Equal(t, "public.example.com:8848", connectedTo)
	assert.Equal(t, 2, runCount)
}

func TestRunScriptWithTraceKeepsLocalNotLeaderTargetWhenCurrentConnectionIsLocal(t *testing.T) {
	originalRunInternal := runInternalRequest
	originalConnectToAddress := connectToAddress
	defer func() {
		runInternalRequest = originalRunInternal
		connectToAddress = originalConnectToAddress
	}()

	runCount := 0
	runInternalRequest = func(c *conn, params *requestParams) (*responseHeader, model.DataForm, error) {
		runCount++
		if runCount == 1 {
			return nil, nil, newServerError("<NotLeader>10.0.0.2:8848:dnode2")
		}
		return nil, nil, nil
	}

	var connectedTo string
	connectToAddress = func(c *conn, addr string) error {
		connectedTo = addr
		return nil
	}

	rawConn, err := NewConn(context.TODO(), "10.0.0.1:8848", nil)
	require.NoError(t, err)
	c := rawConn.(*conn)
	c.isConnected = true
	c.nodePool = newNodePool("10.0.0.1:8848", nil)
	c.publicNameByAddress = map[string]string{
		"10.0.0.2:8848": "public.example.com:8848",
	}
	c.addressByPublicName = map[string]string{
		"public.example.com:8848": "10.0.0.2:8848",
	}

	_, trace, err := c.RunScriptWithTrace("tableInsert(t, 1)")
	require.NoError(t, err)
	require.NotNil(t, trace)
	require.Len(t, trace.Failovers, 1)
	assert.Equal(t, "10.0.0.2:8848", trace.Failovers[0].To)
	assert.Equal(t, "10.0.0.2:8848", connectedTo)
	assert.Equal(t, 2, runCount)
}

func TestRunScriptWithTraceRetriesLocalTargetWhenMappedPublicNameFails(t *testing.T) {
	originalRunInternal := runInternalRequest
	originalConnectToAddress := connectToAddress
	defer func() {
		runInternalRequest = originalRunInternal
		connectToAddress = originalConnectToAddress
	}()

	runCount := 0
	runInternalRequest = func(c *conn, params *requestParams) (*responseHeader, model.DataForm, error) {
		runCount++
		if runCount == 1 {
			return nil, nil, newServerError("<NotLeader>10.0.0.2:8848:dnode2")
		}
		return nil, nil, nil
	}

	var connectedTo []string
	connectToAddress = func(c *conn, addr string) error {
		connectedTo = append(connectedTo, addr)
		if addr == "public.example.com:8848" {
			return net.ErrClosed
		}
		return nil
	}

	rawConn, err := NewConn(context.TODO(), "10.0.0.1:8848", nil)
	require.NoError(t, err)
	c := rawConn.(*conn)
	c.isConnected = true
	c.nodePool = newNodePool("10.0.0.1:8848", nil)
	c.isPublicName = true
	c.publicNameByAddress = map[string]string{
		"10.0.0.2:8848": "public.example.com:8848",
	}
	c.addressByPublicName = map[string]string{
		"public-1.example.com:8848": "10.0.0.1:8848",
		"public.example.com:8848":   "10.0.0.2:8848",
	}

	_, trace, err := c.RunScriptWithTrace("tableInsert(t, 1)")
	require.NoError(t, err)
	require.NotNil(t, trace)
	require.Len(t, trace.Failovers, 2)
	assert.Equal(t, FailoverReasonNotLeader, trace.Failovers[0].Reason)
	assert.Equal(t, "public.example.com:8848", trace.Failovers[0].To)
	assert.Equal(t, FailoverReasonServerDirectedFallback, trace.Failovers[1].Reason)
	assert.Equal(t, "public.example.com:8848", trace.Failovers[1].From)
	assert.Equal(t, "10.0.0.2:8848", trace.Failovers[1].To)
	assert.Equal(t, []string{"public.example.com:8848", "10.0.0.2:8848"}, connectedTo)
	assert.Equal(t, 2, runCount)
}

func TestRunScriptWithTraceRetriesPublicNameTargetWhenMappedLocalAddressFails(t *testing.T) {
	originalRunInternal := runInternalRequest
	originalConnectToAddress := connectToAddress
	defer func() {
		runInternalRequest = originalRunInternal
		connectToAddress = originalConnectToAddress
	}()

	runCount := 0
	runInternalRequest = func(c *conn, params *requestParams) (*responseHeader, model.DataForm, error) {
		runCount++
		if runCount == 1 {
			return nil, nil, newServerError("<NotLeader>10.0.0.2:8848:dnode2")
		}
		return nil, nil, nil
	}

	var connectedTo []string
	connectToAddress = func(c *conn, addr string) error {
		connectedTo = append(connectedTo, addr)
		if addr == "10.0.0.2:8848" {
			return net.ErrClosed
		}
		return nil
	}

	rawConn, err := NewConn(context.TODO(), "10.0.0.1:8848", nil)
	require.NoError(t, err)
	c := rawConn.(*conn)
	c.isConnected = true
	c.nodePool = newNodePool("10.0.0.1:8848", nil)
	c.publicNameByAddress = map[string]string{
		"10.0.0.2:8848": "public.example.com:8848",
	}
	c.addressByPublicName = map[string]string{
		"public.example.com:8848": "10.0.0.2:8848",
	}

	_, trace, err := c.RunScriptWithTrace("tableInsert(t, 1)")
	require.NoError(t, err)
	require.NotNil(t, trace)
	require.Len(t, trace.Failovers, 2)
	assert.Equal(t, FailoverReasonNotLeader, trace.Failovers[0].Reason)
	assert.Equal(t, "10.0.0.2:8848", trace.Failovers[0].To)
	assert.Equal(t, FailoverReasonServerDirectedFallback, trace.Failovers[1].Reason)
	assert.Equal(t, "10.0.0.2:8848", trace.Failovers[1].From)
	assert.Equal(t, "public.example.com:8848", trace.Failovers[1].To)
	assert.Equal(t, []string{"10.0.0.2:8848", "public.example.com:8848"}, connectedTo)
	assert.Equal(t, 2, runCount)
}

func TestRunScriptWithTraceReturnsObservedFailoverWhenSwitchFails(t *testing.T) {
	originalRunInternal := runInternalRequest
	originalConnectToAddress := connectToAddress
	defer func() {
		runInternalRequest = originalRunInternal
		connectToAddress = originalConnectToAddress
	}()

	runInternalRequest = func(c *conn, params *requestParams) (*responseHeader, model.DataForm, error) {
		return nil, nil, newServerError("<NotLeader>10.0.0.2:8848:dnode2")
	}
	switchErr := errors.New("target unavailable")
	connectToAddress = func(c *conn, addr string) error {
		return switchErr
	}

	rawConn, err := NewConn(context.TODO(), "10.0.0.1:8848", nil)
	require.NoError(t, err)
	c := rawConn.(*conn)
	c.isConnected = true
	c.nodePool = newNodePool("10.0.0.1:8848", nil)

	_, trace, err := c.RunScriptWithTrace("tableInsert(t, 1)")
	require.ErrorIs(t, err, switchErr)
	require.NotNil(t, trace)
	require.Len(t, trace.Failovers, 1)
	assert.Equal(t, FailoverReasonNotLeader, trace.Failovers[0].Reason)
	assert.Equal(t, "10.0.0.1:8848", trace.Failovers[0].From)
	assert.Equal(t, "10.0.0.2:8848", trace.Failovers[0].To)
}
