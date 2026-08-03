package dialer

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/dolphindb/api-go/v3/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func disableRequestRetryWait(t *testing.T) {
	t.Helper()
	originalSleep := sleepBeforeRequestRetry
	originalJitter := requestRetryJitter
	sleepBeforeRequestRetry = func(time.Duration) {}
	requestRetryJitter = func() float64 { return 0.5 }
	t.Cleanup(func() {
		sleepBeforeRequestRetry = originalSleep
		requestRetryJitter = originalJitter
	})
}

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
	disableRequestRetryWait(t)
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

func TestRunFuncWithTraceRetriesRepeatedNotLeaderTarget(t *testing.T) {
	disableRequestRetryWait(t)
	originalRunInternal := runInternalRequest
	originalConnectToAddress := connectToAddress
	defer func() {
		runInternalRequest = originalRunInternal
		connectToAddress = originalConnectToAddress
	}()

	runCount := 0
	runInternalRequest = func(c *conn, params *requestParams) (*responseHeader, model.DataForm, error) {
		runCount++
		if runCount == 3 {
			return nil, nil, nil
		}
		return nil, nil, newServerError("<NotLeader>10.0.0.2:8848:dnode2")
	}

	connectCount := 0
	connectToAddress = func(c *conn, addr string) error {
		connectCount++
		c.sessionID = []byte("new-session")
		c.connectedAddress = addr
		return nil
	}

	rawConn, err := NewConn(context.TODO(), "10.0.0.1:8848", &BehaviorOptions{Reconnect: true})
	require.NoError(t, err)
	c := rawConn.(*conn)
	c.isConnected = true
	c.sessionID = []byte("old-session")
	c.nodePool = newNodePool("10.0.0.1:8848", nil)

	_, trace, err := c.RunFuncWithTrace("tableInsert{t}", nil)
	require.NoError(t, err)
	require.NotNil(t, trace)
	require.Len(t, trace.Failovers, 2)
	assert.Equal(t, FailoverReasonNotLeader, trace.Failovers[0].Reason)
	assert.Equal(t, "10.0.0.2:8848", trace.Failovers[0].To)
	assert.Equal(t, FailoverReasonLeaderConvergenceWait, trace.Failovers[1].Reason)
	assert.Equal(t, "10.0.0.2:8848", trace.Failovers[1].From)
	assert.Equal(t, "10.0.0.2:8848", trace.Failovers[1].To)
	assert.Equal(t, 3, runCount)
	assert.Equal(t, 1, connectCount)
}

func TestRunWithTraceRetriesSameNodeInPlaceWithoutChangingSession(t *testing.T) {
	disableRequestRetryWait(t)
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
			return nil, nil, newServerError("<NotLeader>10.0.0.1:8848:dnode1")
		}
		return nil, nil, nil
	}
	connectCount := 0
	connectToAddress = func(c *conn, addr string) error {
		connectCount++
		return nil
	}

	rawConn, err := NewConn(context.TODO(), "10.0.0.1:8848", &BehaviorOptions{Reconnect: true})
	require.NoError(t, err)
	c := rawConn.(*conn)
	c.isConnected = true
	c.connectedAddress = "10.0.0.1:8848"
	c.sessionID = []byte("same-session")
	c.nodePool = newNodePool("10.0.0.1:8848", nil)

	_, trace, err := c.RunScriptWithTrace("tableInsert(t, 1)")
	require.NoError(t, err)
	require.Len(t, trace.Failovers, 1)
	assert.Equal(t, FailoverReasonLeaderConvergenceWait, trace.Failovers[0].Reason)
	assert.Equal(t, "10.0.0.1:8848", trace.Failovers[0].From)
	assert.Equal(t, "10.0.0.1:8848", trace.Failovers[0].To)
	assert.Equal(t, 0, connectCount)
	assert.Equal(t, "same-session", c.GetSession())
	assert.Equal(t, 2, runCount)
}

func TestRunWithTraceStopsAfterLeaderConvergenceRetryLimit(t *testing.T) {
	disableRequestRetryWait(t)
	originalRunInternal := runInternalRequest
	defer func() { runInternalRequest = originalRunInternal }()

	runCount := 0
	runInternalRequest = func(c *conn, params *requestParams) (*responseHeader, model.DataForm, error) {
		runCount++
		return nil, nil, newServerError("<NotLeader>10.0.0.1:8848:dnode1")
	}

	rawConn, err := NewConn(context.TODO(), "10.0.0.1:8848", &BehaviorOptions{Reconnect: true})
	require.NoError(t, err)
	c := rawConn.(*conn)
	c.isConnected = true
	c.connectedAddress = "10.0.0.1:8848"
	c.nodePool = newNodePool("10.0.0.1:8848", nil)

	_, trace, err := c.RunScriptWithTrace("tableInsert(t, 1)")
	require.Error(t, err)
	serverErr, ok := AsServerError(err)
	require.True(t, ok)
	assert.True(t, serverErr.Is(ServerErrNotLeader))
	assert.Equal(t, maxLeaderConvergenceRetries+2, runCount)
	require.Len(t, trace.Failovers, maxLeaderConvergenceRetries+2)
	notLeaderCount := 0
	waitCount := 0
	for _, failover := range trace.Failovers {
		switch failover.Reason {
		case FailoverReasonNotLeader:
			notLeaderCount++
		case FailoverReasonLeaderConvergenceWait:
			waitCount++
		}
	}
	assert.Zero(t, notLeaderCount)
	assert.Equal(t, maxLeaderConvergenceRetries+2, waitCount)
}

func TestRunWithTraceStopsWhenLeaderConvergenceTimeoutWins(t *testing.T) {
	originalRunInternal := runInternalRequest
	originalSleep := sleepBeforeRequestRetry
	originalNow := requestRetryNow
	originalJitter := requestRetryJitter
	defer func() {
		runInternalRequest = originalRunInternal
		sleepBeforeRequestRetry = originalSleep
		requestRetryNow = originalNow
		requestRetryJitter = originalJitter
	}()

	now := time.Unix(100, 0)
	requestRetryNow = func() time.Time { return now }
	requestRetryJitter = func() float64 { return 0.5 }
	sleepBeforeRequestRetry = func(delay time.Duration) { now = now.Add(delay) }
	runCount := 0
	runInternalRequest = func(c *conn, params *requestParams) (*responseHeader, model.DataForm, error) {
		runCount++
		return nil, nil, newServerError("<NotLeader>10.0.0.1:8848:dnode1")
	}

	rawConn, err := NewConn(context.TODO(), "10.0.0.1:8848", &BehaviorOptions{
		Reconnect:                true,
		LeaderConvergenceTimeout: 500 * time.Millisecond,
	})
	require.NoError(t, err)
	c := rawConn.(*conn)
	c.isConnected = true
	c.connectedAddress = "10.0.0.1:8848"
	c.nodePool = newNodePool("10.0.0.1:8848", nil)

	_, trace, err := c.RunScriptWithTrace("tableInsert(t, 1)")
	require.Error(t, err)
	assert.Equal(t, 3, runCount)
	require.Len(t, trace.Failovers, 3)
	assert.Equal(t, FailoverReasonLeaderConvergenceWait, trace.Failovers[0].Reason)
	assert.Equal(t, FailoverReasonLeaderConvergenceWait, trace.Failovers[1].Reason)
	assert.Equal(t, FailoverReasonLeaderConvergenceWait, trace.Failovers[2].Reason)
	assert.Equal(t, 500*time.Millisecond, now.Sub(time.Unix(100, 0)))
}

func TestLeaderConvergenceBackoffAndJitterBounds(t *testing.T) {
	originalJitter := requestRetryJitter
	defer func() { requestRetryJitter = originalJitter }()

	assert.Equal(t, 300*time.Millisecond, leaderConvergenceBackoff(1))
	assert.Equal(t, 600*time.Millisecond, leaderConvergenceBackoff(2))
	assert.Equal(t, time.Second, leaderConvergenceBackoff(3))
	assert.Equal(t, time.Second, leaderConvergenceBackoff(20))

	requestRetryJitter = func() float64 { return 0 }
	assert.Equal(t, 240*time.Millisecond, jitteredRequestRetryDelay(300*time.Millisecond))
	requestRetryJitter = func() float64 { return 1 }
	assert.Equal(t, 360*time.Millisecond, jitteredRequestRetryDelay(300*time.Millisecond))
}

func TestRunWithTraceCountsLocalAndPublicNameAsSameTarget(t *testing.T) {
	disableRequestRetryWait(t)
	originalRunInternal := runInternalRequest
	defer func() { runInternalRequest = originalRunInternal }()

	runCount := 0
	runInternalRequest = func(c *conn, params *requestParams) (*responseHeader, model.DataForm, error) {
		runCount++
		target := "10.0.0.1:8848"
		if runCount%2 == 0 {
			target = "public.example.com:8848"
		}
		return nil, nil, newServerError("<NotLeader>" + target + ":dnode1")
	}

	rawConn, err := NewConn(context.TODO(), "10.0.0.1:8848", &BehaviorOptions{Reconnect: true})
	require.NoError(t, err)
	c := rawConn.(*conn)
	c.isConnected = true
	c.connectedAddress = "10.0.0.1:8848"
	c.nodePool = newNodePool("10.0.0.1:8848", nil)
	c.publicNameByAddress = map[string]string{"10.0.0.1:8848": "public.example.com:8848"}
	c.addressByPublicName = map[string]string{"public.example.com:8848": "10.0.0.1:8848"}

	_, _, err = c.RunScriptWithTrace("tableInsert(t, 1)")
	require.Error(t, err)
	assert.Equal(t, maxLeaderConvergenceRetries+2, runCount)
}

func TestRunWithTraceHonorsTryReconnectNumsDuringConvergence(t *testing.T) {
	disableRequestRetryWait(t)
	originalRunInternal := runInternalRequest
	defer func() { runInternalRequest = originalRunInternal }()

	runCount := 0
	runInternalRequest = func(c *conn, params *requestParams) (*responseHeader, model.DataForm, error) {
		runCount++
		return nil, nil, newServerError("<NotLeader>10.0.0.1:8848:dnode1")
	}
	retries := 1
	rawConn, err := NewConn(context.TODO(), "10.0.0.1:8848", &BehaviorOptions{
		Reconnect:        true,
		TryReconnectNums: &retries,
	})
	require.NoError(t, err)
	c := rawConn.(*conn)
	c.isConnected = true
	c.connectedAddress = "10.0.0.1:8848"
	c.nodePool = newNodePool("10.0.0.1:8848", nil)

	_, _, err = c.RunScriptWithTrace("tableInsert(t, 1)")
	require.Error(t, err)
	assert.Equal(t, 3, runCount)
}

func TestRunWithTraceAllowsABPingPongBeforeLeaderCConverges(t *testing.T) {
	disableRequestRetryWait(t)
	originalRunInternal := runInternalRequest
	originalConnectToAddress := connectToAddress
	defer func() {
		runInternalRequest = originalRunInternal
		connectToAddress = originalConnectToAddress
	}()

	targets := make([]string, 0, 2*(maxLeaderConvergenceRetries+1)+1)
	for i := 0; i <= maxLeaderConvergenceRetries; i++ {
		targets = append(targets, "10.0.0.1:8848", "10.0.0.2:8848")
	}
	targets = append(targets, "10.0.0.3:8848")
	runCount := 0
	runInternalRequest = func(c *conn, params *requestParams) (*responseHeader, model.DataForm, error) {
		if runCount == len(targets) {
			runCount++
			return nil, nil, nil
		}
		target := targets[runCount]
		runCount++
		return nil, nil, newServerError("<NotLeader>" + target + ":leader")
	}
	connectCount := 0
	connectToAddress = func(c *conn, addr string) error {
		connectCount++
		c.connectedAddress = addr
		return nil
	}

	rawConn, err := NewConn(context.TODO(), "10.0.0.3:8848", &BehaviorOptions{Reconnect: true})
	require.NoError(t, err)
	c := rawConn.(*conn)
	c.isConnected = true
	c.connectedAddress = "10.0.0.3:8848"
	c.nodePool = newNodePool("10.0.0.3:8848", nil)

	_, trace, err := c.RunScriptWithTrace("tableInsert(t, 1)")
	require.NoError(t, err)
	assert.Equal(t, len(targets)+1, runCount)
	assert.Equal(t, len(targets), connectCount)
	require.Len(t, trace.Failovers, len(targets))
	for _, failover := range trace.Failovers {
		assert.Equal(t, FailoverReasonNotLeader, failover.Reason)
	}
	assert.Equal(t, "10.0.0.3:8848", trace.Failovers[len(trace.Failovers)-1].To)
}

func TestRunWithTraceDoesNotTreatAmbiguousIOErrorsAsLeaderConvergence(t *testing.T) {
	originalRunInternal := runInternalRequest
	originalHealthCheck := connectionIsHealthy
	originalNow := requestRetryNow
	defer func() {
		runInternalRequest = originalRunInternal
		connectionIsHealthy = originalHealthCheck
		requestRetryNow = originalNow
	}()

	tests := []struct {
		name string
		err  error
	}{
		{name: "EOF", err: io.EOF},
		{name: "read timeout", err: &net.DNSError{Err: "read timeout", IsTimeout: true}},
		{name: "connection reset", err: errors.New("connection reset by peer")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runCount := 0
			nowCount := 0
			runInternalRequest = func(c *conn, params *requestParams) (*responseHeader, model.DataForm, error) {
				runCount++
				return nil, nil, tc.err
			}
			connectionIsHealthy = func(c *conn) bool { return true }
			requestRetryNow = func() time.Time {
				nowCount++
				return time.Unix(100, 0)
			}

			rawConn, err := NewConn(context.TODO(), "10.0.0.1:8848", &BehaviorOptions{
				EnableHighAvailability: true,
				HighAvailabilitySites:  []string{"10.0.0.2:8848"},
			})
			require.NoError(t, err)
			c := rawConn.(*conn)
			c.isConnected = true
			c.connectedAddress = "10.0.0.1:8848"
			c.nodePool = newNodePool("10.0.0.1:8848", []string{"10.0.0.2:8848"})

			_, trace, runErr := c.RunScriptWithTrace("tableInsert(t, 1)")
			require.Error(t, runErr)
			assert.Equal(t, 1, runCount)
			assert.Zero(t, nowCount)
			assert.Empty(t, trace.Failovers)
		})
	}
}

func TestRunWithTraceKeepsTargetlessLeaderErrorsOnHighAvailabilityPath(t *testing.T) {
	disableRequestRetryWait(t)
	originalRunInternal := runInternalRequest
	originalConnectToAddress := connectToAddress
	originalNow := requestRetryNow
	defer func() {
		runInternalRequest = originalRunInternal
		connectToAddress = originalConnectToAddress
		requestRetryNow = originalNow
	}()

	tests := []struct {
		name   string
		detail string
	}{
		{name: "unknown leader", detail: "<UnknownLeader>election in progress"},
		{name: "data node unavailable", detail: "<DataNodeNotAvail>node unavailable"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runCount := 0
			nowCount := 0
			runInternalRequest = func(c *conn, params *requestParams) (*responseHeader, model.DataForm, error) {
				runCount++
				if runCount == 1 {
					return nil, nil, newServerError(tc.detail)
				}
				return nil, nil, nil
			}
			connectToAddress = func(c *conn, addr string) error {
				c.connectedAddress = addr
				return nil
			}
			requestRetryNow = func() time.Time {
				nowCount++
				return time.Unix(100, 0)
			}

			rawConn, err := NewConn(context.TODO(), "10.0.0.1:8848", &BehaviorOptions{
				EnableHighAvailability: true,
				HighAvailabilitySites:  []string{"10.0.0.2:8848"},
			})
			require.NoError(t, err)
			c := rawConn.(*conn)
			c.isConnected = true
			c.connectedAddress = "10.0.0.1:8848"
			c.nodePool = newNodePool("10.0.0.1:8848", []string{"10.0.0.2:8848"})

			_, trace, err := c.RunScriptWithTrace("tableInsert(t, 1)")
			require.NoError(t, err)
			assert.Equal(t, 2, runCount)
			assert.Zero(t, nowCount)
			require.Len(t, trace.Failovers, 1)
			assert.Equal(t, FailoverReasonHighAvailability, trace.Failovers[0].Reason)
		})
	}
}

func TestRunScriptWithTraceRecordsMappedNotLeaderFailover(t *testing.T) {
	disableRequestRetryWait(t)
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
	disableRequestRetryWait(t)
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
	disableRequestRetryWait(t)
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
	disableRequestRetryWait(t)
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
	disableRequestRetryWait(t)
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
