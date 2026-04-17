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
	// RunFile sends script from a specific file to dolphindb and returns the execution result
	RunFile(path string) (model.DataForm, error)
	// RunFunc sends function request to dolphindb and returns the execution result.
	// See DolphinDB function and command references: https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/FunctionReferences/index.html
	RunFunc(s string, args []model.DataForm) (model.DataForm, error)
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

// NewConn instantiates a new connection with the addr.
// BehaviorOpt will affect every request sent by conn.
// You can input opts to configure conn.
func NewConn(ctx context.Context, addr string, behaviorOpt *BehaviorOptions) (Conn, error) {
	if behaviorOpt == nil {
		return &conn{
			behaviorOpt: behaviorOpt,
			addr:        addr,
			timeout:     defaultTimeout,
		}, nil
	}
	if behaviorOpt.EnableHighAvailability && len(behaviorOpt.HighAvailabilitySites) == 0 {
		return nil, errors.New("if EnableHighAvailability is true, HighAvailabilitySites should be specified")
	}
	if !behaviorOpt.EnableHighAvailability && len(behaviorOpt.HighAvailabilitySites) != 0 {
		return nil, errors.New("HighAvailabilitySites requires EnableHighAvailability to be true")
	}
	if behaviorOpt.Priority != nil && (*behaviorOpt.Priority < 0 || *behaviorOpt.Priority > 8) {
		return nil, errors.New("the job priority must be between 0 and 8")
	}
	timeout := behaviorOpt.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	} else if timeout < 0 {
		return nil, errors.New("the Timeout must be non-negative")
	}
	if behaviorOpt.NetTimeout < 0 {
		return nil, errors.New("the NetTimeout must be non-negative")
	}
	highAvailabilitySites := shuffledSites(behaviorOpt.HighAvailabilitySites)
	return &conn{
		behaviorOpt:            behaviorOpt,
		addr:                   addr,
		timeout:                timeout,
		highAvailabilitySites:  highAvailabilitySites,
		enableHighAvailability: behaviorOpt.EnableHighAvailability,
		loadBalance:            behaviorOpt.LoadBalance,
		reconnect:              behaviorOpt.Reconnect,
	}, nil
}

// NewSimpleConn instantiates a new connection with the addr,
// which connects to the server and logs in with the userID and pwd.
func NewSimpleConn(ctx context.Context, address, userID, pwd string) (Conn, error) {
	return NewSimpleConnWithBehavior(ctx, address, userID, pwd, nil)
}

// NewSimpleConnWithBehavior instantiates a new connection with the addr,
// which connects to the server and logs in with the userID and pwd.
func NewSimpleConnWithBehavior(ctx context.Context, address, userID, pwd string, behaviorOpt *BehaviorOptions) (Conn, error) {
	conn, err := NewConn(ctx, address, behaviorOpt)
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

	return strings.Split(c.LocalAddr().String(), ":")[0]
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

	remainingAttempts := c.getRetryTimes()
	if minAttemptsForFailover := c.nodePool.len - 1; remainingAttempts < minAttemptsForFailover {
		remainingAttempts = minAttemptsForFailover
	}

	return c.switchDataNodeWithAttempts(nil, remainingAttempts)
}

func (c *conn) connectWithReconnect() error {
	c.nodePool = newNodePool(c.addr, nil)
	return c.switchDataNode(nil)
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
	copied := append([]string(nil), sites...)
	if len(copied) < 2 {
		return copied
	}

	shuffleStringSlice(copied)
	return copied
}

func (c *conn) connect(addr string) error {
	dc, err := dialWithDialer(&net.Dialer{Timeout: c.connectTimeout()}, "tcp", addr)
	if err != nil {
		dialerLogf("Failed to connect to %s: %v", addr, err)
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
		dialerLogf("failed to configure tcp socket options for %s: %v", addr, err)
		return err
	}

	h, _, err := c.runInternal(&requestParams{
		commandType: connectCmd,
		Command:     generateConnectionCommand(),
	})

	if err != nil {
		dialerLogf("session handshake with %s failed: %v", addr, err)
		return err
	}

	c.isConnected = true
	c.isClosed = false
	c.refreshHeaderForResponse(h)

	args := make([]model.DataForm, 0)
	ret, err := c.runFuncInternal("isNodeInitialized", args)
	if err != nil {
		dialerLogf("server %s does not support node initialization check", addr)
	} else {
		if !(ret.(*model.Scalar)).Value().(bool) {
			c.isConnected = false
			dialerLogf("connected to %s, but the node is not initialized yet", addr)
			return fmt.Errorf("<DataNodeNotReady>") // use a special error to indicate that the node is not initialized
		}
	}
	err = nil

	if c.userID != "" || c.password != "" {
		err = loginWithCredentials(c, c.userID, c.password)
		if err != nil {
			dialerLogf("login to %s failed: %v", addr, err)
		} else {
			dialerLogf("login to %s succeeded for user %q", addr, c.userID)
		}
	}

	if err == nil {
		dialerLogf("connected to %s (session=%s)", addr, c.GetSession())
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
	_, di, err := c.run(&requestParams{
		commandType: scriptCmd,
		Command:     generateScriptCommand(s),
	})

	return di, err
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
	bo := defaultByteOrder

	_, di, err := c.run(&requestParams{
		commandType: functionCmd,
		Command:     generateFunctionCommand(s, bo, args),
		SessionID:   []byte(c.GetSession()),
		Args:        args,
		ByteOrder:   bo,
	})

	return di, err
}

func (c *conn) runFuncInternal(s string, args []model.DataForm) (model.DataForm, error) {
	bo := defaultByteOrder

	_, di, err := c.runInternal(&requestParams{
		commandType: functionCmd,
		Command:     generateFunctionCommand(s, bo, args),
		SessionID:   []byte(c.GetSession()),
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
		SessionID:   []byte(c.GetSession()),
		Args:        args,
		ByteOrder:   bo,
	})

	return di, err
}

func (c *conn) run(params *requestParams) (*responseHeader, model.DataForm, error) {
	if c.nodePool == nil || c.nodePool.len <= 0 {
		return c.runInternal(params)
	}

	rh, df, err := c.runInternal(params)
	for i := 0; i <= c.getRetryTimes(); i++ {
		if err == nil {
			return rh, df, nil
		}

		currentNode := c.nodePool.nodes[c.nodePool.lastInd]
		failedAddr := currentNode.address
		dialerLogf("request on %s failed: %v", failedAddr, err)
		et := c.nodePool.parseError(err, currentNode)

		if et == UNKNOWN && c.isConnected && c.connected() {
			return rh, df, err
		}
		if et == LOGIN_REQUIRED {
			return rh, df, err
		}

		n := currentNode
		if !(et == NEW_LEADER || et == NO_INITIALIZED || et == NODE_NOT_AVAIL) {
			// not use the current node
			n = nil
		}
		if n != nil {
			dialerLogf("request failure on %s maps to server-directed retry target %s", failedAddr, n.address)
		} else {
			dialerLogf("request failure on %s will trigger high-availability failover", failedAddr)
		}
		time.Sleep(300 * time.Millisecond)

		// for loop would break when switchDataNode run out of retry times
		if err := c.switchDataNode(n); err != nil {
			return nil, nil, err
		}
		rh, df, err = c.runInternal(params)
	}
	return rh, df, err
}

func (c *conn) runInternal(params *requestParams) (*responseHeader, model.DataForm, error) {
	if !c.isConnected && params.commandType != connectCmd {
		return nil, nil, errors.New("database connection is not established yet")
	}

	if params.commandType == scriptCmd || params.commandType == functionCmd || params.commandType == connectCmd {
		if c.behaviorOpt == nil {
			c.behaviorOpt = &BehaviorOptions{}
		}

		if c.behaviorOpt.GetFetchSize() > 0 && c.behaviorOpt.GetFetchSize() < 8192 {
			return nil, nil, fmt.Errorf("fetchSize %d must be equal or greater than 8192", c.behaviorOpt.GetFetchSize())
		}
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	err := c.SetWriteDeadline(time.Now().Add(c.timeout))
	if err != nil {
		return nil, nil, err
	}

	w := protocol.NewWriter(c.Conn)
	err = writeRequest(w, params, c.behaviorOpt)
	if err != nil {
		c.isConnected = false
		dialerLogf("request write to %s failed: %v", c.currentRemoteAddress(), err)
		return nil, nil, err
	}

	err = c.SetReadDeadline(time.Now().Add(c.timeout))
	if err != nil {
		return nil, nil, err
	}

	h, di, err := c.parseResponse(c.reader)
	if err != nil {
		dialerLogf("response read from %s failed: %v", c.currentRemoteAddress(), err)
		return nil, nil, err
	}

	return h, di, nil
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

	dialerLogf("SCRAM login succeeded")
	return nil
}
