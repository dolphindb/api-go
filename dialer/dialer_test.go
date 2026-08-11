package dialer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dolphindb/api-go/v3/logging"
	"github.com/dolphindb/api-go/v3/model"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testAddr = "127.0.0.1:3002"

func TestDialer(t *testing.T) {
	fOpt, err := normalizeBehaviorOptions(&BehaviorOptions{})
	require.NoError(t, err)
	require.NotNil(t, fOpt.Priority)
	require.NotNil(t, fOpt.Parallelism)
	require.NotNil(t, fOpt.FetchSize)
	assert.Equal(t, defaultParallelism, *fOpt.Parallelism)
	assert.Equal(t, defaultPriority, *fOpt.Priority)
	assert.Equal(t, defaultFetchSize, *fOpt.FetchSize)

	priority := 2
	parallelism := 4
	fetchSize := 100
	fOpt, err = normalizeBehaviorOptions(&BehaviorOptions{
		Priority:    &priority,
		Parallelism: &parallelism,
		FetchSize:   &fetchSize,
	})
	require.NoError(t, err)
	assert.Equal(t, parallelism, *fOpt.Parallelism)
	assert.Equal(t, priority, *fOpt.Priority)
	assert.Equal(t, fetchSize, *fOpt.FetchSize)

	_, err = NewConn(context.TODO(), testAddr, nil)
	assert.Nil(t, err)

	c, err := Dial(testAddr, "user", "password", nil)
	assert.Nil(t, err)

	connected, err := Dial(testAddr, "user", "password", nil)
	assert.Nil(t, err)
	assert.True(t, connected.IsConnected())
	assert.Nil(t, connected.Close())

	// c.AddInitScript("schema()")
	// assert.Equal(t, c.GetInitScripts(), []string{"schema()"})

	// c.SetInitScripts([]string{"init", "login"})
	// assert.Equal(t, c.GetInitScripts(), []string{"init", "login"})

	c.RefreshTimeout(10 * time.Second)

	err = c.Connect()
	assert.Nil(t, err)
	assert.Equal(t, c.IsClosed(), false)

	f, err := os.Create("test.txt")
	assert.Nil(t, err)

	_, err = f.Write([]byte("login"))
	assert.Nil(t, err)

	err = f.Close()
	assert.Nil(t, err)

	_, err = c.RunFile("./test.txt")
	assert.Nil(t, err)

	err = os.Remove("./test.txt")
	assert.Nil(t, err)

	dt, err := model.NewDataType(model.DtString, "test")
	assert.Nil(t, err)

	s := model.NewScalar(dt)
	_, err = c.RunFunc("typestr", []model.DataForm{s})
	assert.Nil(t, err)

	df, err := c.Upload(map[string]model.DataForm{"scalar": s})
	assert.Nil(t, err)
	assert.Equal(t, c.GetSession(), "20267359")
	assert.Equal(t, df.GetDataForm(), model.DfScalar)
	assert.Equal(t, df.GetDataType(), model.DtString)
	assert.Equal(t, df.String(), "string(OK)")

	address := c.GetLocalAddress()
	assert.True(t, strings.HasPrefix(address, "127.0.0.1"))

	err = c.Close()
	assert.Nil(t, err)
	assert.True(t, c.IsClosed())
}

func TestReconnectRetriesUntilServerComesBack(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.Nil(t, err)
	addr := ln.Addr().String()
	assert.Nil(t, ln.Close())

	exit := make(chan bool)
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			close(exit)
		})
	}
	defer cleanup()

	go func() {
		time.Sleep(1500 * time.Millisecond)
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			return
		}
		defer listener.Close()

		go func() {
			<-exit
			_ = listener.Close()
		}()

		for !isExit(exit) {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			go handleData(conn)
		}
	}()

	retries := 5
	conn, err := NewConn(context.TODO(), addr, &BehaviorOptions{
		Reconnect:        true,
		TryReconnectNums: &retries,
	})
	assert.Nil(t, err)

	start := time.Now()
	err = conn.Connect()
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, time.Since(start), time.Second)

	assert.Nil(t, conn.Close())
}

func TestNewConnRejectsNegativeNetTimeout(t *testing.T) {
	conn, err := NewConn(context.TODO(), testAddr, &BehaviorOptions{
		NetTimeout: -time.Second,
	})

	assert.Nil(t, conn)
	assert.EqualError(t, err, "the NetTimeout must be non-negative")
}

func TestNewConnRejectsHighAvailabilitySitesWhenHighAvailabilityDisabled(t *testing.T) {
	conn, err := NewConn(context.TODO(), testAddr, &BehaviorOptions{
		EnableHighAvailability: false,
		HighAvailabilitySites:  []string{"127.0.0.1:3003"},
	})

	assert.Nil(t, conn)
	assert.EqualError(t, err, "HighAvailabilitySites requires EnableHighAvailability to be true")
}

type retryableNetError struct{}

func (retryableNetError) Error() string   { return "temporary network error" }
func (retryableNetError) Timeout() bool   { return false }
func (retryableNetError) Temporary() bool { return true }

func TestConnectWithHighAvailabilityTriesPrimaryAddressBeforeFallbackSites(t *testing.T) {
	original := connectToAddress
	defer func() {
		connectToAddress = original
	}()
	originalShuffle := shuffleStringSlice
	defer func() {
		shuffleStringSlice = originalShuffle
	}()
	shuffleStringSlice = func(values []string) {
		for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
			values[i], values[j] = values[j], values[i]
		}
	}

	var attempted []string
	connectToAddress = func(c *conn, addr string) error {
		attempted = append(attempted, addr)
		if addr == "primary:8848" {
			return retryableNetError{}
		}

		return nil
	}

	c, err := NewConn(context.TODO(), "primary:8848", &BehaviorOptions{
		EnableHighAvailability: true,
		HighAvailabilitySites:  []string{"primary:8848", "secondary:8848", "tertiary:8848"},
	})
	assert.Nil(t, err)

	err = c.Connect()
	assert.Nil(t, err)
	assert.Equal(t, []string{"primary:8848", "tertiary:8848"}, attempted)
}

func TestConnectWithHighAvailabilityDoesNotFallbackWhenPrimarySucceeds(t *testing.T) {
	original := connectToAddress
	defer func() {
		connectToAddress = original
	}()
	originalShuffle := shuffleStringSlice
	defer func() {
		shuffleStringSlice = originalShuffle
	}()
	shuffleStringSlice = func(values []string) {}

	var attempted []string
	connectToAddress = func(c *conn, addr string) error {
		attempted = append(attempted, addr)
		return nil
	}

	c, err := NewConn(context.TODO(), "primary:8848", &BehaviorOptions{
		EnableHighAvailability: true,
		HighAvailabilitySites:  []string{"secondary:8848", "tertiary:8848"},
	})
	assert.Nil(t, err)

	err = c.Connect()
	assert.Nil(t, err)
	assert.Equal(t, []string{"primary:8848"}, attempted)
}

func TestConnectWithHighAvailabilityFallsBackWhenPrimaryReturnsError(t *testing.T) {
	original := connectToAddress
	defer func() {
		connectToAddress = original
	}()
	originalShuffle := shuffleStringSlice
	defer func() {
		shuffleStringSlice = originalShuffle
	}()
	shuffleStringSlice = func(values []string) {}

	var attempted []string
	connectToAddress = func(c *conn, addr string) error {
		attempted = append(attempted, addr)
		if addr == "primary:8848" {
			return errors.New("unexpected primary failure")
		}

		return nil
	}

	c, err := NewConn(context.TODO(), "primary:8848", &BehaviorOptions{
		EnableHighAvailability: true,
		HighAvailabilitySites:  []string{"secondary:8848", "tertiary:8848"},
	})
	assert.Nil(t, err)

	err = c.Connect()
	assert.Nil(t, err)
	assert.Equal(t, []string{"primary:8848", "secondary:8848"}, attempted)
}

func TestConnectWithHighAvailabilityReconnectCountsDoNotDoubleCountPrimary(t *testing.T) {
	originalConnect := connectToAddress
	defer func() {
		connectToAddress = originalConnect
	}()
	originalShuffle := shuffleStringSlice
	defer func() {
		shuffleStringSlice = originalShuffle
	}()
	originalSleep := sleepBeforeRetry
	defer func() {
		sleepBeforeRetry = originalSleep
	}()
	shuffleStringSlice = func(values []string) {}
	sleepBeforeRetry = func(time.Duration) {}

	var attempted []string
	connectToAddress = func(c *conn, addr string) error {
		attempted = append(attempted, addr)
		return retryableNetError{}
	}

	retries := 3
	c, err := NewConn(context.TODO(), "primary:8848", &BehaviorOptions{
		Reconnect:              true,
		EnableHighAvailability: true,
		HighAvailabilitySites:  []string{"primary:8848", "secondary:8848", "tertiary:8848", "quaternary:8848"},
		TryReconnectNums:       &retries,
	})
	require.NoError(t, err)

	err = c.Connect()
	require.EqualError(t, err, "failed to connect to primary:8848")
	assert.Equal(t, []string{"primary:8848", "secondary:8848", "tertiary:8848", "quaternary:8848"}, attempted)
}

func TestConnectWithHighAvailabilityStillTriesEachNodeOnceWhenRetryCountIsSmallerThanFallbackCount(t *testing.T) {
	originalConnect := connectToAddress
	defer func() {
		connectToAddress = originalConnect
	}()
	originalShuffle := shuffleStringSlice
	defer func() {
		shuffleStringSlice = originalShuffle
	}()
	originalSleep := sleepBeforeRetry
	defer func() {
		sleepBeforeRetry = originalSleep
	}()
	shuffleStringSlice = func(values []string) {}
	sleepBeforeRetry = func(time.Duration) {}

	var attempted []string
	connectToAddress = func(c *conn, addr string) error {
		attempted = append(attempted, addr)
		return retryableNetError{}
	}

	retries := 1
	c, err := NewConn(context.TODO(), "primary:8848", &BehaviorOptions{
		EnableHighAvailability: true,
		HighAvailabilitySites:  []string{"secondary:8848", "tertiary:8848"},
		TryReconnectNums:       &retries,
	})
	require.NoError(t, err)

	err = c.Connect()
	require.EqualError(t, err, "failed to connect to primary:8848")
	assert.Equal(t, []string{"primary:8848", "secondary:8848", "tertiary:8848"}, attempted)
}

func TestSwitchDataNodeLogsFinalFailedAttempt(t *testing.T) {
	originalConnect := connectToAddress
	defer func() {
		connectToAddress = originalConnect
	}()
	originalSleep := sleepBeforeRetry
	defer func() {
		sleepBeforeRetry = originalSleep
	}()
	sleepBeforeRetry = func(time.Duration) {}

	var logBuf bytes.Buffer
	originalLogger := logging.Logger()
	defer logging.SetLogger(originalLogger)
	logging.SetLogger(slog.New(slog.NewTextHandler(&logBuf, nil)))

	connectToAddress = func(c *conn, addr string) error {
		return retryableNetError{}
	}

	rawConn, err := NewConn(context.TODO(), "primary:8848", &BehaviorOptions{
		Reconnect: true,
	})
	require.NoError(t, err)

	internalConn := rawConn.(*conn)
	internalConn.nodePool = newNodePool("primary:8848", nil)

	_, err = internalConn.switchDataNodeWithAttempts(nil, 3)
	require.EqualError(t, err, "failed to connect to primary:8848")
	assert.Contains(t, logBuf.String(), "failover attempt 3/3 did not connect; no retries left")
}

func TestConnectWithReconnectRetriesIndefinitelyWhenRetryCountIsNil(t *testing.T) {
	originalConnect := connectToAddress
	defer func() {
		connectToAddress = originalConnect
	}()
	originalSleep := sleepBeforeRetry
	defer func() {
		sleepBeforeRetry = originalSleep
	}()
	sleepBeforeRetry = func(time.Duration) {}

	attempts := 0
	connectToAddress = func(c *conn, addr string) error {
		attempts++
		if attempts < 2 {
			return retryableNetError{}
		}
		return nil
	}

	rawConn, err := NewConn(context.TODO(), "primary:8848", &BehaviorOptions{
		Reconnect: true,
	})
	require.NoError(t, err)

	err = rawConn.Connect()
	require.NoError(t, err)
	assert.Equal(t, 2, attempts)
}

func TestReconnectConnectDoesNotStartFailoverWhenPrimaryConnects(t *testing.T) {
	originalConnect := connectToAddress
	defer func() {
		connectToAddress = originalConnect
	}()
	connectToAddress = func(c *conn, addr string) error {
		return nil
	}

	var logBuf bytes.Buffer
	originalLogger := logging.Logger()
	defer logging.SetLogger(originalLogger)
	logging.SetLogger(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))

	rawConn, err := NewConn(context.TODO(), "primary:8848", &BehaviorOptions{
		Reconnect: true,
	})
	require.NoError(t, err)

	require.NoError(t, rawConn.Connect())
	logs := logBuf.String()
	assert.NotContains(t, logs, "starting failover attempt")
	assert.Contains(t, logs, "connection to primary:8848 is ready")
}

func TestNewConnShufflesHighAvailabilitySitesWithoutMutatingInput(t *testing.T) {
	originalShuffle := shuffleStringSlice
	defer func() {
		shuffleStringSlice = originalShuffle
	}()
	shuffleStringSlice = func(values []string) {
		for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
			values[i], values[j] = values[j], values[i]
		}
	}

	sites := []string{"secondary:8848", "tertiary:8848", "quaternary:8848"}
	behaviorOpt := &BehaviorOptions{
		EnableHighAvailability: true,
		HighAvailabilitySites:  sites,
	}

	c, err := NewConn(context.TODO(), "primary:8848", behaviorOpt)
	assert.Nil(t, err)

	internalConn := c.(*conn)
	assert.Equal(t, []string{"quaternary:8848", "tertiary:8848", "secondary:8848"}, internalConn.highAvailabilitySites)
	assert.Equal(t, []string{"secondary:8848", "tertiary:8848", "quaternary:8848"}, behaviorOpt.HighAvailabilitySites)
}

func TestConnectUsesConfiguredNetTimeout(t *testing.T) {
	original := dialWithDialer
	defer func() {
		dialWithDialer = original
	}()

	var gotTimeout time.Duration
	dialWithDialer = func(d *net.Dialer, network, address string) (net.Conn, error) {
		gotTimeout = d.Timeout
		return nil, retryableNetError{}
	}

	rawConn, err := NewConn(context.TODO(), testAddr, &BehaviorOptions{
		NetTimeout: 2 * time.Second,
	})
	require.NoError(t, err)

	err = rawConn.(*conn).connect(testAddr)
	require.Error(t, err)
	assert.Equal(t, 2*time.Second, gotTimeout)
}

func TestConnectNodeDoesNotTreatEINVALAsRetryable(t *testing.T) {
	originalConnect := connectToAddress
	defer func() {
		connectToAddress = originalConnect
	}()
	connectToAddress = func(c *conn, addr string) error {
		return fmt.Errorf("NetTimeout too long (720h0m0s): %w", &net.OpError{
			Op:  "set",
			Net: "tcp",
			Err: &os.SyscallError{
				Syscall: "setsockopt",
				Err:     syscall.EINVAL,
			},
		})
	}

	rawConn, err := NewConn(context.TODO(), "primary:8848", nil)
	require.NoError(t, err)

	connected, err := rawConn.(*conn).connectNode(&node{address: "primary:8848"})
	require.False(t, connected)
	require.EqualError(t, err, "NetTimeout too long (720h0m0s): set tcp: setsockopt: invalid argument")
}

func TestConnectLogsSuccessfulLoginWithUserName(t *testing.T) {
	addr, stop := startDialerTestServer(t)
	defer stop()

	originalLogin := loginWithCredentials
	defer func() {
		loginWithCredentials = originalLogin
	}()

	var logBuf bytes.Buffer
	originalLogger := logging.Logger()
	defer logging.SetLogger(originalLogger)
	logging.SetLogger(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))

	var loginCalls int
	loginWithCredentials = func(conn Conn, userID, password string) error {
		loginCalls++
		assert.Equal(t, "alice", userID)
		assert.Equal(t, "secret", password)
		return nil
	}

	rawConn, err := NewConn(context.TODO(), addr, nil)
	require.NoError(t, err)
	rawConn.SetUserID("alice")
	rawConn.SetPassword("secret")

	require.NoError(t, rawConn.Connect())
	defer rawConn.Close()

	assert.Equal(t, 1, loginCalls)
	assert.Contains(t, logBuf.String(), "login to "+addr+" succeeded for user")
	assert.Contains(t, logBuf.String(), "alice")
}

func TestConnectSilencesOptionalScramUnavailableBeforePasswordLogin(t *testing.T) {
	addr, stop := startDialerTestServer(t)
	defer stop()

	var logBuf bytes.Buffer
	originalLogger := logging.Logger()
	defer logging.SetLogger(originalLogger)
	logging.SetLogger(slog.New(slog.NewTextHandler(&logBuf, nil)))

	rawConn, err := NewConn(context.TODO(), addr, nil)
	require.NoError(t, err)
	rawConn.SetUserID("alice")
	rawConn.SetPassword("secret")

	require.NoError(t, rawConn.Connect())
	defer rawConn.Close()

	logs := logBuf.String()
	assert.NotContains(t, logs, "level=WARN")
	assert.NotContains(t, logs, "level=ERROR")
	assert.NotContains(t, logs, "scramClientFirst")
	assert.NotContains(t, logs, "SCRAM login is unavailable")
	assert.Contains(t, logs, "login to "+addr+" succeeded for user")
}

func TestSwitchDataNodeReconnectsWithLogin(t *testing.T) {
	addr, stop := startDialerTestServer(t)
	defer stop()

	originalLogin := loginWithCredentials
	defer func() {
		loginWithCredentials = originalLogin
	}()

	var loginTargets []string
	loginWithCredentials = func(dialConn Conn, userID, password string) error {
		loginTargets = append(loginTargets, dialConn.(*conn).currentRemoteAddress())
		return nil
	}

	retries := 1
	rawConn, err := NewConn(context.TODO(), addr, &BehaviorOptions{
		Reconnect:        true,
		TryReconnectNums: &retries,
	})
	require.NoError(t, err)
	rawConn.SetUserID("alice")
	rawConn.SetPassword("secret")

	require.NoError(t, rawConn.Connect())

	internalConn := rawConn.(*conn)
	require.NoError(t, internalConn.Close())
	_, err = internalConn.switchDataNode(nil)
	require.NoError(t, err)
	defer internalConn.Close()

	assert.Equal(t, []string{addr, addr}, loginTargets)
}

func TestSwitchDataNodeHighAvailabilityRelogsOnNewNode(t *testing.T) {
	primaryAddr, stopPrimary := startDialerTestServer(t)
	defer stopPrimary()
	secondaryAddr, stopSecondary := startDialerTestServer(t)
	defer stopSecondary()

	originalLogin := loginWithCredentials
	defer func() {
		loginWithCredentials = originalLogin
	}()

	var loginTargets []string
	loginWithCredentials = func(dialConn Conn, userID, password string) error {
		loginTargets = append(loginTargets, dialConn.(*conn).currentRemoteAddress())
		return nil
	}

	retries := 1
	rawConn, err := NewConn(context.TODO(), primaryAddr, &BehaviorOptions{
		EnableHighAvailability: true,
		HighAvailabilitySites:  []string{secondaryAddr},
		TryReconnectNums:       &retries,
	})
	require.NoError(t, err)
	rawConn.SetUserID("alice")
	rawConn.SetPassword("secret")

	require.NoError(t, rawConn.Connect())

	internalConn := rawConn.(*conn)
	require.NoError(t, internalConn.Close())
	_, err = internalConn.switchDataNode(&node{address: secondaryAddr})
	require.NoError(t, err)
	defer internalConn.Close()

	assert.Equal(t, []string{primaryAddr, secondaryAddr}, loginTargets)
}

func TestConnSocketOptionsDefaultsAndDisables(t *testing.T) {
	rawConn, err := NewConn(context.TODO(), testAddr, nil)
	require.NoError(t, err)

	opt := rawConn.(*conn).tcpSocketOptions()
	assert.Equal(t, defaultKeepAliveTime, opt.keepAliveTime)
	assert.Equal(t, defaultTCPUserTimeout, opt.tcpUserTimeout)
	assert.Equal(t, defaultTCPUserTimeout/defaultKeepAliveProbeCnt, opt.keepAliveInterval)
	assert.Equal(t, defaultKeepAliveProbeCnt, opt.keepAliveCount)

	rawConn, err = NewConn(context.TODO(), testAddr, &BehaviorOptions{
		NetTimeout: 9 * time.Second,
	})
	require.NoError(t, err)

	opt = rawConn.(*conn).tcpSocketOptions()
	assert.Equal(t, 9*time.Second, opt.keepAliveTime)
	assert.Equal(t, 9*time.Second, opt.tcpUserTimeout)
	assert.Equal(t, 3*time.Second, opt.keepAliveInterval)
	assert.Equal(t, defaultKeepAliveProbeCnt, opt.keepAliveCount)
}

func TestSqlStdEnumString(t *testing.T) {
	assert.Equal(t, "DolphinDB", SqlStdDolphinDB.String())
	assert.Equal(t, "Oracle", SqlStdOracle.String())
	assert.Equal(t, "MySQL", SqlStdMySQL.String())
	assert.Equal(t, "Unknown", SqlStdEnum(99).String())
}

func TestCloseWithoutUnderlyingConn(t *testing.T) {
	c := &conn{}

	assert.Nil(t, c.Close())
	assert.True(t, c.IsClosed())
	assert.False(t, c.IsConnected())
}

func TestNormalizeBehaviorOptionsUsesNilAsInternalDefaults(t *testing.T) {
	opt, err := normalizeBehaviorOptions(&BehaviorOptions{})
	require.NoError(t, err)
	assert.Equal(t, defaultPriority, *opt.Priority)
	assert.Equal(t, defaultParallelism, *opt.Parallelism)
	assert.Equal(t, defaultFetchSize, *opt.FetchSize)

	positive := 4
	opt, err = normalizeBehaviorOptions(&BehaviorOptions{TryReconnectNums: &positive})
	require.NoError(t, err)
	require.NotNil(t, opt.TryReconnectNums)
	assert.Equal(t, 4, *opt.TryReconnectNums)
}

func TestBehaviorOptionsValidateRejectsNonPositiveTryReconnectNums(t *testing.T) {
	zero := 0
	negative := -1

	require.EqualError(t, (&BehaviorOptions{TryReconnectNums: &zero}).Validate(), "TryReconnectNums must be nil or greater than 0")
	require.EqualError(t, (&BehaviorOptions{TryReconnectNums: &negative}).Validate(), "TryReconnectNums must be nil or greater than 0")
	require.NoError(t, (&BehaviorOptions{}).Validate())

	positive := 2
	require.NoError(t, (&BehaviorOptions{TryReconnectNums: &positive}).Validate())
}

func TestGetRetryLimitUsesNilAsUnlimitedReconnects(t *testing.T) {
	positive := 2

	assert.Nil(t, (&conn{behaviorOpt: &BehaviorOptions{Reconnect: true}}).getRetryLimit())
	require.NotNil(t, (&conn{behaviorOpt: &BehaviorOptions{Reconnect: true, TryReconnectNums: &positive}}).getRetryLimit())
	assert.Equal(t, 2, *(&conn{behaviorOpt: &BehaviorOptions{Reconnect: true, TryReconnectNums: &positive}}).getRetryLimit())

	limit := (&conn{}).getRetryLimit()
	require.NotNil(t, limit)
	assert.Equal(t, 0, *limit)
}

func TestNewConnRejectsNonPositiveTryReconnectNums(t *testing.T) {
	zero := 0
	negative := -1

	_, err := NewConn(context.TODO(), testAddr, &BehaviorOptions{
		Reconnect:        true,
		TryReconnectNums: &zero,
	})
	require.EqualError(t, err, "TryReconnectNums must be nil or greater than 0")

	_, err = NewConn(context.TODO(), testAddr, &BehaviorOptions{
		Reconnect:        true,
		TryReconnectNums: &negative,
	})
	require.EqualError(t, err, "TryReconnectNums must be nil or greater than 0")
}

func TestMain(m *testing.M) {
	exit := make(chan bool)
	ln, err := net.Listen("tcp", testAddr)
	if err != nil {
		return
	}
	go func() {
		for !isExit(exit) {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			go handleData(conn)
		}

		ln.Close()
	}()

	exitCode := m.Run()

	close(exit)

	os.Exit(exitCode)
}

func handleData(conn net.Conn) {
	const (
		successResponse        = "20267359 0 1\nOK\n"
		boolScalarTrueResponse = "20267359 1 1\nOK\n\x01\x00\x01"
		intScalarTwoResponse   = "20267359 1 1\nOK\n\x04\x00\x02\x00\x00\x00"
		scramUnavailableError  = "20267359 0 1\nCan't recognize function name scramClientFirst\n"
		stringScalarOKResponse = "20267359 1 1\nOK\n\x12\x00OK\x00"
	)

	res := make([]byte, 0)
	for {
		buf := make([]byte, 512)
		l, err := conn.Read(buf)
		if err != nil {
			continue
		}

		res = append(res, buf[0:l]...)
		script := string(res)
		if strings.Contains(script, "scramClientFirst") {
			_, err = conn.Write([]byte(scramUnavailableError))
			if err != nil {
				return
			}

			res = make([]byte, 0)
		} else if strings.Contains(script, "isNodeInitialized") {
			_, err = conn.Write([]byte(boolScalarTrueResponse))
			if err != nil {
				return
			}

			res = make([]byte, 0)
		} else if strings.Contains(script, "login") {
			_, err = conn.Write([]byte(successResponse))
			if err != nil {
				return
			}

			res = make([]byte, 0)
		} else if strings.Contains(script, "typestr") {
			_, err = conn.Write([]byte(successResponse))
			if err != nil {
				return
			}

			res = make([]byte, 0)
		} else if strings.Contains(script, "1+1") {
			_, err = conn.Write([]byte(intScalarTwoResponse))
			if err != nil {
				return
			}

			res = make([]byte, 0)
		} else if strings.Contains(script, "variable\nscalar\n1") {
			_, err = conn.Write([]byte(stringScalarOKResponse))
			if err != nil {
				return
			}

			res = make([]byte, 0)
		} else if len(res) == 25 || len(res) == 27 || len(res) == 29 || len(res) == 30 || len(res) == 48 ||
			len(res) == 54 || len(res) == 49 {
			_, err = conn.Write([]byte(successResponse))
			if err != nil {
				return
			}

			res = make([]byte, 0)
		} else if len(res) == 42 {
			_, err = conn.Write([]byte(stringScalarOKResponse))
			if err != nil {
				return
			}

			res = make([]byte, 0)
		}
	}
}

func isExit(exit <-chan bool) bool {
	select {
	case <-exit:
		return true
	default:
		return false
	}
}

func startDialerTestServer(t *testing.T) (string, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-done:
					return
				default:
					return
				}
			}

			go handleTestProtocolConn(conn)
		}
	}()

	return ln.Addr().String(), func() {
		close(done)
		_ = ln.Close()
	}
}

func handleTestProtocolConn(conn net.Conn) {
	defer conn.Close()

	const (
		successResponse        = "20267359 0 1\nOK\n"
		boolScalarTrueResponse = "20267359 1 1\nOK\n\x01\x00\x01"
		intScalarTwoResponse   = "20267359 1 1\nOK\n\x04\x00\x02\x00\x00\x00"
		scramUnavailableError  = "20267359 0 1\nCan't recognize function name scramClientFirst\n"
		stringScalarOKResponse = "20267359 1 1\nOK\n\x12\x00OK\x00"
	)

	res := make([]byte, 0)
	for {
		buf := make([]byte, 512)
		l, err := conn.Read(buf)
		if err != nil {
			return
		}

		res = append(res, buf[:l]...)
		if strings.Contains(string(res), "scramClientFirst") {
			if _, err = conn.Write([]byte(scramUnavailableError)); err != nil {
				return
			}
			res = res[:0]
			continue
		}

		if strings.Contains(string(res), "isNodeInitialized") {
			if _, err = conn.Write([]byte(boolScalarTrueResponse)); err != nil {
				return
			}
			res = res[:0]
			continue
		}

		if strings.Contains(string(res), "login") {
			if _, err = conn.Write([]byte(successResponse)); err != nil {
				return
			}
			res = res[:0]
			continue
		}

		if strings.Contains(string(res), "typestr") {
			if _, err = conn.Write([]byte(successResponse)); err != nil {
				return
			}
			res = res[:0]
			continue
		}

		if strings.Contains(string(res), "1+1") {
			if _, err = conn.Write([]byte(intScalarTwoResponse)); err != nil {
				return
			}
			res = res[:0]
			continue
		}

		if strings.Contains(string(res), "variable\nscalar\n1") {
			if _, err = conn.Write([]byte(stringScalarOKResponse)); err != nil {
				return
			}
			res = res[:0]
			continue
		}

		switch len(res) {
		case 25, 27, 29, 30, 48, 49, 54:
			if _, err = conn.Write([]byte(successResponse)); err != nil {
				return
			}
			res = res[:0]
		case 42:
			if _, err = conn.Write([]byte(stringScalarOKResponse)); err != nil {
				return
			}
			res = res[:0]
		}
	}
}
