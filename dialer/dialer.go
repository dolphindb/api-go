package dialer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/dolphindb/api-go/model"
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

	GetReader() protocol.Reader
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
		fmt.Println("Warn: HighAvailabilitySites is not empty but EnableHighAvailability is false")
	}
	return &conn{
		behaviorOpt:            behaviorOpt,
		addr:                   addr,
		timeout:                defaultTimeout,
		highAvailabilitySites:  behaviorOpt.HighAvailabilitySites,
		enableHighAvailability: behaviorOpt.EnableHighAvailability,
		loadBalance:            behaviorOpt.LoadBalance,
		reconnect:              behaviorOpt.Reconnect,
	}, nil
}

// NewSimpleConn instantiates a new connection with the addr,
// which connects to the server and logs in with the userID and pwd.
func NewSimpleConn(ctx context.Context, address, userID, pwd string) (Conn, error) {
	conn, err := NewConn(ctx, address, nil)
	if err != nil {
		return nil, err
	}

	conn.SetPassword(pwd)
	conn.SetUserID(userID)

	err = conn.Connect()
	if err != nil {
		return nil, err
	}

	_, err = conn.RunScript(fmt.Sprintf("login('%s','%s')", userID, pwd))
	if err != nil {
		return nil, err
	}

	return conn, err
}

func (c *conn) GetReader() protocol.Reader  {
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
		c.nodePool = &nodePool{
			nodes: make([]*node, 0),
		}

		c.nodePool.add(&node{address: c.addr})
		for _, v := range c.highAvailabilitySites {
			c.nodePool.add(&node{address: v})
		}

		_, err := c.connectMinNode()
		return err
	} else {
		if c.reconnect {
			c.nodePool = &nodePool{
				nodes: []*node{{address: c.addr}},
			}
			return c.switchDatanode(&node{address: ""})
		} else {
			ok, err := c.connectNode(&node{address: c.addr})
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("failed to connect to %s", c.addr)
			}
		}
	}

	return nil
}

func (c *conn) connect(addr string) error {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}

	dc, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return err
	}

	err = dc.SetKeepAlivePeriod(30 * time.Second)
	if err != nil {
		return err
	}

	c.reader = protocol.NewReader(dc)
	c.Conn = dc
	h, _, err := c.run(&requestParams{
		commandType: connectCmd,
		Command:     generateConnectionCommand(),
	})
	if err != nil {
		return err
	}

	c.isConnected = true
	c.isClosed = false
	c.refreshHeaderForResponse(h)
	if c.userID != "" {
		_, err = c.RunScript(fmt.Sprintf("login('%s','%s')", c.userID, c.password))
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *conn) Close() error {
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
	if c.nodePool != nil && c.nodePool.len > 0 {
		for {
			rh, df, err := c.runInternal(params)
			if err != nil {
				n := &node{}
				if c.connected() {
					et := c.nodePool.parseError(err.Error(), n)
					if et == IGNORE {
						return rh, df, nil
					} else if et == UNKNOW {
						return nil, nil, err
					}
				}
				c.switchDatanode(n)
				continue
			}

			return rh, df, nil
		}
	} else {
		return c.runInternal(params)
	}
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
			return nil, nil, fmt.Errorf("fetchSize %d must be greater than 8192", c.behaviorOpt.GetFetchSize())
		}
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	err := c.SetDeadline(time.Now().Add(c.timeout))
	if err != nil {
		return nil, nil, err
	}

	w := protocol.NewWriter(c.Conn)
	err = writeRequest(w, params, c.behaviorOpt)
	if err != nil {
		c.isConnected = false
		return nil, nil, err
	}

	h, di, err := c.parseResponse(c.reader)
	if err != nil {
		return nil, nil, err
	}

	return h, di, nil
}

func (c *conn) refreshHeaderForResponse(h *responseHeader) {
	c.sessionID = h.sessionID
}
