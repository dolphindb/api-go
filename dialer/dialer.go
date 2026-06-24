package dialer

import (
	"context"
	"crypto/hmac"
	cryptoRand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	mathrand "math/rand"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dolphindb/api-go/v3/dialer/protocol"
	"github.com/dolphindb/api-go/v3/model"
	"golang.org/x/crypto/pbkdf2"
)

const (
	defaultByteOrder = protocol.LittleEndianByte
	defaultTimeout   = time.Minute
)

const (
	connectCmd  = "connect"
	scriptCmd   = "script"
	functionCmd = "function"
	variableCmd = "variable"
)

// Conn is the interface of DolphinDB conn.
type Conn interface {
	net.Conn

	// Connect connects to dolphindb server
	Connect() error
	// GetLocalAddress gets the local address with the connection
	GetLocalAddress() string

	//  RefreshTimeout resets the timeout of the connection
	RefreshTimeout(t time.Duration)
	// GetSession gets the session id of the connection
	GetSession() string
	// Close closes the connection with server
	Close() error
	// IsClosed checks whether the connection is closed
	IsClosed() bool
	// IsConnected checks whether the connection is connected
	IsConnected() bool
	// AddInitScript(script string)
	// SetInitScripts(scripts []string)
	// GetInitScripts() []string

	// GetUserID gets the userID
	GetUserID() string
	// SetUserID sets the userID
	SetUserID(userID string)
	// GetPassword gets the password
	GetPassword() string
	// SetPassword sets the password
	SetPassword(password string)

	// RunScript sends script to dolphindb and returns the execution result
	RunScript(s string) (model.DataForm, error)
	// RunScriptWithTrace sends script to dolphindb and returns execution trace information.
	RunScriptWithTrace(s string) (model.DataForm, *ExecutionTrace, error)
	// RunFile sends script from a specific file to dolphindb and returns the execution result
	RunFile(path string) (model.DataForm, error)
	// RunFunc sends function request to dolphindb and returns the execution result.
	// See DolphinDB function and command references: https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/FunctionReferences/index.html
	RunFunc(s string, args []model.DataForm) (model.DataForm, error)
	// RunFuncWithTrace sends function request to dolphindb and returns execution trace information.
	RunFuncWithTrace(s string, args []model.DataForm) (model.DataForm, *ExecutionTrace, error)
	// Upload sends local objects to dolphindb server and the specified variable is generated on the dolphindb
	Upload(vars map[string]model.DataForm) (model.DataForm, error)
	// GetTCPConn returns the TCPConn
	GetTCPConn() *net.TCPConn

	// for inner use
	GetReader() protocol.Reader
	enableScram() bool
	ConnLogin(userID, password string) error
}

type conn struct {
	lock sync.Mutex

	net.Conn
	reader                 protocol.Reader
	behaviorOpt            *BehaviorOptions
	sessionID              []byte
	isConnected            bool
	isClosed               bool
	loadBalance            bool
	enableHighAvailability bool
	reconnect              bool
	nodePool               *nodePool
	//	initScripts []string
	highAvailabilitySites []string

	userID, password, addr string
	timeout                time.Duration
	logName                string
	publicNameByAddress    map[string]string
	addressByPublicName    map[string]string
	isPublicName           bool
}

var shuffleStringSlice = func(values []string) {
	r := mathrand.New(mathrand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(values), func(i, j int) {
		values[i], values[j] = values[j], values[i]
	})
}

var dialWithDialer = func(d *net.Dialer, network, address string) (net.Conn, error) {
	return d.Dial(network, address)
}

// runInternalRequest is a test seam for request retry and trace tests.
var runInternalRequest = func(c *conn, params *requestParams) (*responseHeader, model.DataForm, error) {
	return c.runInternal(params)
}

// NewConn instantiates a new connection with the addr.
// BehaviorOpt will affect every request sent by conn.
// You can input opts to configure conn.
func NewConn(ctx context.Context, addr string, behaviorOpt *BehaviorOptions) (Conn, error) {
	normalizedBehaviorOpt, err := normalizeBehaviorOptions(behaviorOpt)
	if err != nil {
		return nil, err
	}
	if normalizedBehaviorOpt.EnableHighAvailability && len(normalizedBehaviorOpt.HighAvailabilitySites) == 0 {
		return nil, errors.New("if EnableHighAvailability is true, HighAvailabilitySites should be specified")
	}
	if !normalizedBehaviorOpt.EnableHighAvailability && len(normalizedBehaviorOpt.HighAvailabilitySites) != 0 {
		return nil, errors.New("HighAvailabilitySites requires EnableHighAvailability to be true")
	}
	if *normalizedBehaviorOpt.Priority < 0 || *normalizedBehaviorOpt.Priority > 8 {
		return nil, errors.New("the job priority must be between 0 and 8")
	}
	timeout := normalizedBehaviorOpt.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	} else if timeout < 0 {
		return nil, errors.New("the Timeout must be non-negative")
	}
	if normalizedBehaviorOpt.NetTimeout < 0 {
		return nil, errors.New("the NetTimeout must be non-negative")
	}
	highAvailabilitySites := shuffledSites(normalizedBehaviorOpt.HighAvailabilitySites)
	return &conn{
		behaviorOpt:            normalizedBehaviorOpt,
		addr:                   addr,
		timeout:                timeout,
		highAvailabilitySites:  highAvailabilitySites,
		enableHighAvailability: normalizedBehaviorOpt.EnableHighAvailability,
		loadBalance:            normalizedBehaviorOpt.LoadBalance,
		reconnect:              normalizedBehaviorOpt.Reconnect,
	}, nil
}

// Deprecated: use Dial instead.
func NewSimpleConn(_ context.Context, address, userID, pwd string) (Conn, error) {
	return Dial(address, userID, pwd, nil)
}

// Deprecated: use Dial instead.
func NewSimpleConnWithBehavior(_ context.Context, address, userID, pwd string, behaviorOpt *BehaviorOptions) (Conn, error) {
	return Dial(address, userID, pwd, behaviorOpt)
}

// Dial creates a connection, connects to the server and logs in with the provided credentials.
// It uses context.Background internally until the connect path supports cancellation end-to-end.
func Dial(address, userID, pwd string, behaviorOpt *BehaviorOptions) (Conn, error) {
	conn, err := NewConn(context.Background(), address, behaviorOpt)
	if err != nil {
		return nil, err
	}

	conn.SetPassword(pwd)
	conn.SetUserID(userID)

	err = conn.Connect()
	if err != nil {
		return nil, err
	}

	return conn, err
}

func (c *conn) GetReader() protocol.Reader {
	return c.reader
}

// SetLogName sets a short diagnostic name attached to connection logs.
func (c *conn) SetLogName(name string) {
	c.logName = name
}

// LogName returns the diagnostic name attached to connection logs.
func (c *conn) LogName() string {
	return c.logName
}

// Add an init script which will be run after you call connect
// func (c *conn) AddInitScript(script string) {
// 	if c.initScripts == nil {
// 		c.initScripts = make([]string, 0)
// 	}
// 	c.initScripts = append(c.initScripts, script)
// }

func (c *conn) GetLocalAddress() string {
	if !c.connected() {
		return ""
	}

	host, _, err := net.SplitHostPort(c.LocalAddr().String())
	if err != nil {
		return ""
	}

	return host
}

func (c *conn) currentRemoteAddress() string {
	if c.Conn != nil && c.Conn.RemoteAddr() != nil {
		return c.Conn.RemoteAddr().String()
	}

	return c.addr
}

// Get init scripts which will be run after you call connect
// func (c *conn) GetInitScripts() []string {
// 	return c.initScripts
// }

// Set init scripts which will be run after you call connect
// func (c *conn) SetInitScripts(scripts []string) {
// 	c.initScripts = scripts
// }

func (c *conn) GetUserID() string {
	return c.userID
}

func (c *conn) SetUserID(userID string) {
	c.userID = userID
}

// func (c *conn) GetEnableHighAvailability() bool {
// 	return c.enableHighAvailability
// }

// func (c *conn) SetEnableHighAvailability(enableHighAvailability bool) {
// 	c.enableHighAvailability = enableHighAvailability
// }

// func (c *conn) GetHighAvailabilitySites() []string {
// 	return c.highAvailabilitySites
// }

// func (c *conn) SetHighAvailabilitySites(highAvailabilitySites []string) {
// 	c.highAvailabilitySites = highAvailabilitySites
// }

// func (c *conn) GetLoadBalance() bool {
// 	return c.loadBalance
// }

// func (c *conn) SetLoadBalance(loadBalance bool) {
// 	c.loadBalance = loadBalance
// }

func (c *conn) GetPassword() string {
	return c.password
}

func (c *conn) SetPassword(password string) {
	c.password = password
}

func (c *conn) RefreshTimeout(t time.Duration) {
	c.timeout = t
}

func (c *conn) GetTCPConn() *net.TCPConn {
	return c.Conn.(*net.TCPConn)
}

func (c *conn) Connect() error {
	if c.enableHighAvailability {
		return c.connectWithHighAvailability()
	}

	if c.reconnect {
		return c.connectWithReconnect()
	}

	return c.connectWithoutFailover()
}

func (c *conn) connectWithHighAvailability() error {
	c.nodePool = newNodePool(c.addr, c.highAvailabilitySites)

	ok, _ := c.connectNode(&node{address: c.addr})
	if ok {
		return nil
	}

	retryLimit := c.getRetryLimit()
	if retryLimit == nil {
		_, err := c.switchDataNodeWithoutLimit(nil)
		return err
	}

	remainingAttempts := *retryLimit
	if minAttemptsForFailover := c.nodePool.len - 1; remainingAttempts < minAttemptsForFailover {
		remainingAttempts = minAttemptsForFailover
	}

	_, err := c.switchDataNodeWithAttempts(nil, remainingAttempts)
	return err
}

func (c *conn) connectWithReconnect() error {
	c.nodePool = newNodePool(c.addr, nil)

	ok, err := c.connectNode(&node{address: c.addr})
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	_, err = c.switchDataNode(nil)
	return err
}

func (c *conn) connectWithoutFailover() error {
	ok, err := c.connectNode(&node{address: c.addr})
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("failed to connect to %s", c.addr)
	}

	return nil
}

func newNodePool(primary string, fallbacks []string) *nodePool {
	pool := &nodePool{
		nodes: make([]*node, 0, 1+len(fallbacks)),
	}
	pool.add(&node{address: primary})
	for _, addr := range fallbacks {
		pool.add(&node{address: addr})
	}

	return pool
}

func shuffledSites(sites []string) []string {
	copied := slices.Clone(sites)
	if len(copied) < 2 {
		return copied
	}

	shuffleStringSlice(copied)
	return copied
}

func (c *conn) connect(addr string) error {
	dc, err := dialWithDialer(&net.Dialer{Timeout: c.connectTimeout()}, "tcp", addr)
	if err != nil {
		c.dialerLogDebugf("failed to connect to %s: %v", addr, err)
		return err
	}

	tcpConn, ok := dc.(*net.TCPConn)
	if !ok {
		_ = dc.Close()
		return fmt.Errorf("expected TCP connection, got %T", dc)
	}

	c.reader = protocol.NewReader(tcpConn)
	c.Conn = tcpConn
	cleanup := true
	defer func() {
		if cleanup {
			_ = tcpConn.Close()
			c.reader = nil
			c.Conn = nil
			c.isConnected = false
			c.isClosed = true
			c.sessionID = nil
		}
	}()

	if err := setTCPSocketOptions(tcpConn, c.tcpSocketOptions()); err != nil {
		c.dialerLogWarnf("failed to configure tcp socket options for %s: %v", addr, err)
		return err
	}

	h, _, err := c.runInternal(&requestParams{
		commandType: connectCmd,
		Command:     generateConnectionCommand(),
	})

	if err != nil {
		c.dialerLogErrorf("session handshake with %s failed: %v", addr, err)
		return err
	}

	c.isConnected = true
	c.isClosed = false
	c.refreshHeaderForResponse(h)

	args := make([]model.DataForm, 0)
	ret, err := c.runFuncInternal("isNodeInitialized", args)
	if err != nil {
		c.dialerLogDebugf("server %s does not support node initialization check", addr)
	} else {
		if !(ret.(*model.Scalar)).Value().(bool) {
			c.isConnected = false
			c.dialerLogWarnf("connected to %s, but the node is not initialized yet", addr)
			return fmt.Errorf("<DataNodeNotReady>") // use a special error to indicate that the node is not initialized
		}
	}
	err = nil

	if c.userID != "" || c.password != "" {
		err = loginWithCredentials(c, c.userID, c.password)
		if err != nil {
			c.dialerLogErrorf("login to %s failed: %v", addr, err)
		} else {
			c.dialerLogDebugf("login to %s succeeded for user %q", addr, c.userID)
		}
	}

	if err == nil {
		c.dialerLogDebugf("connected to %s (session=%s)", addr, c.GetSession())
		c.refreshPublicNameAddressMap(addr)
	}
	cleanup = err != nil
	return err
}

func (c *conn) Close() error {
	if c == nil {
		return nil
	}

	if c.Conn == nil {
		c.isConnected = false
		c.isClosed = true
		c.sessionID = nil
		return nil
	}

	if err := c.Conn.Close(); err != nil {
		return err
	}

	c.isConnected = false
	c.isClosed = true
	c.sessionID = nil

	return nil
}

func (c *conn) IsClosed() bool {
	return c.isClosed
}

func (c *conn) IsConnected() bool {
	return c.isConnected
}

// RunScript sends script to dolphindb and return the execution result.
func (c *conn) RunScript(s string) (model.DataForm, error) {
	di, _, err := c.RunScriptWithTrace(s)
	return di, err
}

// RunScriptWithTrace sends script to dolphindb and returns the execution result
// along with request-level execution trace information.
func (c *conn) RunScriptWithTrace(s string) (model.DataForm, *ExecutionTrace, error) {
	_, di, trace, err := c.runWithTrace(&requestParams{
		commandType: scriptCmd,
		Command:     generateScriptCommand(s),
	})

	return di, trace, err
}

// RunFile sends script from a specific file to dolphindb and return the execution result.
func (c *conn) RunFile(path string) (model.DataForm, error) {
	script, err := readFile(path)
	if err != nil {
		return nil, err
	}

	_, di, err := c.run(&requestParams{
		commandType: scriptCmd,
		Command:     generateScriptCommand(script),
	})

	return di, err
}

// GetSession returns session id.
func (c *conn) GetSession() string {
	return string(c.sessionID)
}

// RunFunc sends function request to dolphindb and return the execution result.
// Refer to https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/FunctionReferences/index.html for more details.
func (c *conn) RunFunc(s string, args []model.DataForm) (model.DataForm, error) {
	di, _, err := c.RunFuncWithTrace(s, args)
	return di, err
}

// RunFuncWithTrace sends function request to dolphindb and returns the execution
// result along with request-level execution trace information.
func (c *conn) RunFuncWithTrace(s string, args []model.DataForm) (model.DataForm, *ExecutionTrace, error) {
	bo := defaultByteOrder

	_, di, trace, err := c.runWithTrace(&requestParams{
		commandType: functionCmd,
		Command:     generateFunctionCommand(s, bo, args),
		Args:        args,
		ByteOrder:   bo,
	})

	return di, trace, err
}

func (c *conn) runFuncInternal(s string, args []model.DataForm) (model.DataForm, error) {
	bo := defaultByteOrder

	_, di, err := c.runInternal(&requestParams{
		commandType: functionCmd,
		Command:     generateFunctionCommand(s, bo, args),
		Args:        args,
		ByteOrder:   bo,
	})

	return di, err
}

// Upload sends local data to dolphindb and the specified variable is generated on the dolphindb.
func (c *conn) Upload(vars map[string]model.DataForm) (model.DataForm, error) {
	bo := defaultByteOrder

	names := make([]string, len(vars))
	count := 0
	args := make([]model.DataForm, len(vars))
	for k, v := range vars {
		if !isVariableCandidate(k) {
			return nil, fmt.Errorf("%s is not a good variable name", k)
		}
		names[count] = k
		args[count] = v
		count++
	}
	_, di, err := c.run(&requestParams{
		commandType: variableCmd,
		Command:     generateVariableCommand(strings.Join(names, ","), bo, count),
		Args:        args,
		ByteOrder:   bo,
	})

	return di, err
}

func (c *conn) run(params *requestParams) (*responseHeader, model.DataForm, error) {
	rh, df, _, err := c.runWithTrace(params)
	return rh, df, err
}

func (c *conn) runWithTrace(params *requestParams) (*responseHeader, model.DataForm, *ExecutionTrace, error) {
	trace := newExecutionTrace()
	if c.nodePool == nil || c.nodePool.len <= 0 {
		rh, df, err := runInternalRequest(c, params)
		return rh, df, trace, err
	}

	rh, df, err := runInternalRequest(c, params)
	retryLimit := c.getRetryLimit()
	retriedTargets := make(map[string]struct{})
	var connectedAddress string
	for attempt := 0; retryLimit == nil || attempt <= *retryLimit; attempt++ {
		if err == nil {
			return rh, df, trace, nil
		}

		currentNode := c.nodePool.nodes[c.nodePool.lastInd]
		failedAddr := currentNode.address
		et, targetAddress := c.nodePool.parseError(err)
		failoverErr := err

		if et == UNKNOWN && c.isConnected && c.connected() {
			return rh, df, trace, err
		}
		if et == LOGIN_REQUIRED {
			return rh, df, trace, err
		}

		fallbackAddress := ""
		var retryNode *node
		if targetAddress != "" {
			rawTargetAddress := targetAddress
			targetPair := c.serverDirectedAddressPair(targetAddress)
			targetAddress, fallbackAddress = targetPair.preferred(c.isPublicName)
			if _, ok := retriedTargets[targetAddress]; ok {
				c.dialerLogWarnf("request failure on %s maps to already retried target %s; stopping retry (reason=%v err=%v rawTarget=%s)", failedAddr, targetAddress, et, failoverErr, rawTargetAddress)
				return rh, df, trace, err
			}
			if fallbackAddress != "" && fallbackAddress != targetAddress {
				if _, ok := retriedTargets[fallbackAddress]; ok {
					c.dialerLogWarnf("request failure on %s maps to already retried fallback %s; stopping retry (reason=%v err=%v rawTarget=%s target=%s)", failedAddr, fallbackAddress, et, failoverErr, rawTargetAddress, targetAddress)
					return rh, df, trace, err
				}
			}
			retryNode = newNodeWithFallback(targetAddress, fallbackAddress)
			c.dialerLogInfof("request failure on %s maps to server-directed retry target %s (reason=%v err=%v rawTarget=%s fallback=%s)", failedAddr, retryNode.address, et, failoverErr, rawTargetAddress, fallbackAddress)
		} else {
			c.dialerLogWarnf("request failure on %s will trigger high-availability failover (reason=%v err=%v)", failedAddr, et, failoverErr)
		}
		time.Sleep(300 * time.Millisecond)

		failoverErrText := ""
		if failoverErr != nil {
			failoverErrText = failoverErr.Error()
		}
		trace.Failovers = append(trace.Failovers, FailoverTrace{
			Reason: failoverReason(targetAddress),
			From:   failedAddr,
			To:     targetAddress,
			Err:    failoverErrText,
		})
		traceIndex := len(trace.Failovers) - 1

		// for loop would break when switchDataNode run out of retry times
		connectedAddress, err = c.switchDataNode(retryNode)
		if err != nil {
			return nil, nil, trace, err
		}
		if connectedAddress == "" {
			if trace.Failovers[traceIndex].To == "" {
				trace.Failovers[traceIndex].To = c.currentRemoteAddress()
			}
		} else if targetAddress == "" || connectedAddress == targetAddress {
			trace.Failovers[traceIndex].To = connectedAddress
		} else if fallbackAddress != "" && connectedAddress == fallbackAddress {
			trace.Failovers = append(trace.Failovers, FailoverTrace{
				Reason: FailoverReasonServerDirectedFallback,
				From:   targetAddress,
				To:     connectedAddress,
				Err:    failoverErrText,
			})
		} else {
			trace.Failovers = append(trace.Failovers, FailoverTrace{
				Reason: FailoverReasonHighAvailability,
				From:   targetAddress,
				To:     connectedAddress,
				Err:    failoverErrText,
			})
		}
		retryAddress := connectedAddress
		if retryAddress == "" {
			retryAddress = trace.Failovers[len(trace.Failovers)-1].To
		}
		if retryAddress != "" {
			retriedTargets[retryAddress] = struct{}{}
		}
		rh, df, err = runInternalRequest(c, params)
		if err == nil {
			c.dialerLogInfof("conn-level retry on %s succeeded", retryAddress)
		} else {
			c.dialerLogWarnf("conn-level retry on %s failed: %v", retryAddress, err)
		}
	}
	return rh, df, trace, err
}

func failoverReason(targetAddress string) FailoverReason {
	if targetAddress != "" {
		return FailoverReasonNotLeader
	}

	return FailoverReasonHighAvailability
}

func (c *conn) runInternal(params *requestParams) (*responseHeader, model.DataForm, error) {
	if !c.isConnected && params.commandType != connectCmd {
		return nil, nil, errors.New("database connection is not established yet")
	}

	if params.commandType == scriptCmd || params.commandType == functionCmd || params.commandType == connectCmd {
		if c.behaviorOpt == nil {
			normalizedBehaviorOpt, normalizeErr := normalizeBehaviorOptions(nil)
			if normalizeErr != nil {
				return nil, nil, normalizeErr
			}
			c.behaviorOpt = normalizedBehaviorOpt
		}

		if *c.behaviorOpt.FetchSize > 0 && *c.behaviorOpt.FetchSize < 8192 {
			return nil, nil, fmt.Errorf("fetchSize %d must be equal or greater than 8192", *c.behaviorOpt.FetchSize)
		}
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	err := c.SetWriteDeadline(time.Now().Add(c.timeout))
	if err != nil {
		return nil, nil, err
	}

	w := protocol.NewWriter(c.Conn)
	err = writeRequest(w, c.prepareRequestParams(params), c.behaviorOpt)
	if err != nil {
		c.isConnected = false
		return nil, nil, err
	}

	err = c.SetReadDeadline(time.Now().Add(c.timeout))
	if err != nil {
		return nil, nil, err
	}

	h, di, err := c.parseResponse(c.reader)
	if h != nil {
		c.refreshHeaderForResponse(h)
	}
	if err != nil {
		return nil, nil, err
	}

	return h, di, nil
}

func (c *conn) prepareRequestParams(params *requestParams) *requestParams {
	prepared := *params
	if params.commandType == connectCmd {
		prepared.SessionID = nil
	} else {
		prepared.SessionID = []byte(c.GetSession())
	}

	return &prepared
}

func (c *conn) refreshHeaderForResponse(h *responseHeader) {
	c.sessionID = h.sessionID
}

func (c *conn) enableScram() bool {
	return c.behaviorOpt.EnableScram
}

func (c *conn) ConnLogin(userID, password string) error {
	if c.enableScram() {
		return c.scramLogin(userID, password)
	} else {
		err := c.scramLogin(userID, password)
		if err == nil {
			return nil
		}
	}
	args := make([]model.DataForm, 2)
	user, err := model.NewDataType(model.DtString, userID)
	if err != nil {
		return err
	}
	pwd, err := model.NewDataType(model.DtString, password)
	if err != nil {
		return err
	}

	args[0] = model.NewScalar(user)
	args[1] = model.NewScalar(pwd)
	_, err = c.runFuncInternal("login", args)
	if err != nil {
		return err
	}
	return nil
}

func generateNonce(length int) (string, error) {
	buffer := make([]byte, length)
	_, err := cryptoRand.Read(buffer)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(buffer), nil
}

func xorBytes(a, b []byte) []byte {
	result := make([]byte, len(a))
	for i := range a {
		result[i] = a[i] ^ b[i]
	}
	return result
}

func (c *conn) scramLogin(userID, password string) error {
	args := make([]model.DataForm, 2)
	user, err := model.NewDataType(model.DtString, userID)
	if err != nil {
		return fmt.Errorf("SCRAM login failed, %w", err)
	}
	clientNonce, err := generateNonce(16)
	if err != nil {
		return fmt.Errorf("SCRAM login failed, %w", err)
	}
	nonce, err := model.NewDataType(model.DtString, clientNonce)
	if err != nil {
		return fmt.Errorf("SCRAM login failed, %w", err)
	}
	args[0] = model.NewScalar(user)
	args[1] = model.NewScalar(nonce)

	result, err := c.runFuncInternal("scramClientFirst", args)
	if err != nil {
		if strings.Contains(err.Error(), "Can't recognize function name scramClientFirst") {
			return fmt.Errorf("SCRAM login is unavailable on current server")
		}
		if strings.Contains(err.Error(), "sha256 authMode doesn't support scram authMode") {
			return fmt.Errorf("user '%s' doesn't support scram authMode", userID)
		}
		return fmt.Errorf("scramClientFirst failed: %w", err)
	}

	retVec := result.(*model.Vector)

	if retVec.Rows() != 3 {
		return fmt.Errorf("SCRAM login failed, server error: get server nonce failed")
	}
	saltStr := retVec.Get(0).Value().(*model.Scalar).Value().(string)
	iterCount := int(retVec.Get(1).Value().(*model.Scalar).Value().(int32))
	combinedNonce := retVec.Get(2).Value().(*model.Scalar).Value().(string)

	salt, err := base64.StdEncoding.DecodeString(saltStr)
	if err != nil {
		return fmt.Errorf("SCRAM login failed, base64 decode failed: %w", err)
	}

	saltedPassword := pbkdf2.Key([]byte(password), salt, iterCount, 32, sha256.New)

	mac := hmac.New(sha256.New, saltedPassword)
	_, err = mac.Write([]byte("Client Key"))
	if err != nil {
		return fmt.Errorf("SCRAM login failed, HMAC calculation failed: %w", err)
	}
	clientKey := mac.Sum(nil)

	storedKey := sha256.Sum256(clientKey)

	authMessage := fmt.Sprintf(`n=%s,r=%s,r=%s,s=%s,i=%d,c=biws,r=%s`,
		userID, clientNonce, combinedNonce, saltStr, iterCount, combinedNonce)

	mac = hmac.New(sha256.New, storedKey[:])
	_, err = mac.Write([]byte(authMessage))
	if err != nil {
		return fmt.Errorf("SCRAM login failed, HMAC calculation failed: %w", err)
	}
	clientSig := mac.Sum(nil)

	proof := xorBytes(clientKey, clientSig)

	finalArgs := make([]model.DataForm, 3)
	combinedNonceScalar, err := model.NewDataType(model.DtString, combinedNonce)
	if err != nil {
		return fmt.Errorf("SCRAM login failed, %w", err)
	}
	proofScalar, err := model.NewDataType(model.DtString, base64.StdEncoding.EncodeToString(proof))
	if err != nil {
		return fmt.Errorf("SCRAM login failed, %w", err)
	}

	finalArgs[0] = model.NewScalar(user)
	finalArgs[1] = model.NewScalar(combinedNonceScalar)
	finalArgs[2] = model.NewScalar(proofScalar)

	finalResult, err := c.runFuncInternal("scramClientFinal", finalArgs)
	if err != nil {
		return fmt.Errorf("scramClientFinal failed: %w", err)
	}
	serverSigBase64 := finalResult.(*model.Scalar).Value().(string)

	mac = hmac.New(sha256.New, saltedPassword)
	_, err = mac.Write([]byte("Server Key"))
	if err != nil {
		return fmt.Errorf("SCRAM login failed, HMAC calculation failed: %w", err)
	}
	serverKey := mac.Sum(nil)

	mac = hmac.New(sha256.New, serverKey)
	_, err = mac.Write([]byte(authMessage))
	if err != nil {
		return fmt.Errorf("SCRAM login failed, HMAC calculation failed: %w", err)
	}
	serverSig := mac.Sum(nil)

	expectedSig := base64.StdEncoding.EncodeToString(serverSig)

	if serverSigBase64 != "" && expectedSig != serverSigBase64 {
		c.Close()
		return fmt.Errorf("invalid SCRAM server signature")
	}

	c.dialerLogDebugf("SCRAM login succeeded")
	return nil
}
