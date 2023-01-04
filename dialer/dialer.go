package dialer

import (
	"context"
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
	// AddInitScript(script string)
	// SetInitScripts(scripts []string)
	// GetInitScripts() []string

	// RunScript sends script to dolphindb and returns the execution result
	RunScript(s string) (model.DataForm, error)
	// RunFile sends script from a specific file to dolphindb and returns the execution result
	RunFile(path string) (model.DataForm, error)
	// RunFunc sends function request to dolphindb and returns the execution result.
	// See DolphinDB function and command references: https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/FunctionReferences/index.html
	RunFunc(s string, args []model.DataForm) (model.DataForm, error)
	// Upload sends local objects to dolphindb server and the specified variable is generated on the dolphindb
	Upload(vars map[string]model.DataForm) (model.DataForm, error)
}

type conn struct {
	lock sync.Mutex

	net.Conn
	reader      protocol.Reader
	behaviorOpt *BehaviorOptions
	sessionID   []byte
	connected   bool
	//	initScripts []string

	timeout time.Duration
}

// BehaviorOptions helps you configure behavior identity.
// Refer to https://github.com/dolphindb/Tutorials_CN/blob/master/api_protocol.md#254-%E8%A1%8C%E4%B8%BA%E6%A0%87%E8%AF%86 for more details.
type BehaviorOptions struct {
	// Priority specifies the priority of the task
	Priority *int
	// Parallelism specifies the parallelism of the task
	Parallelism *int
	// FetchSize specifies the fetchSize of the task
	FetchSize *int
}

// SetPriority sets the priority of the task.
func (f *BehaviorOptions) SetPriority(p int) *BehaviorOptions {
	f.Priority = &p
	return f
}

// SetParallelism sets the parallelism of the task.
func (f *BehaviorOptions) SetParallelism(p int) *BehaviorOptions {
	f.Parallelism = &p
	return f
}

// SetFetchSize sets the fetchSize of the task.
func (f *BehaviorOptions) SetFetchSize(fs int) *BehaviorOptions {
	f.FetchSize = &fs
	return f
}

// GetPriority gets the priority of the task.
func (f *BehaviorOptions) GetPriority() int {
	if f.Priority == nil {
		return 4
	}
	return *f.Priority
}

// GetParallelism gets the parallelism of the task.
func (f *BehaviorOptions) GetParallelism() int {
	if f.Parallelism == nil {
		return 2
	}
	return *f.Parallelism
}

// GetFetchSize gets the fetchSize of the task.
func (f *BehaviorOptions) GetFetchSize() int {
	if f.FetchSize == nil {
		return 0
	}
	return *f.FetchSize
}

// NewConn instantiates a new connection with the addr.
// BehaviorOpt will affect every request sent by conn.
// You can input opts to configure conn.
func NewConn(ctx context.Context, addr string, behaviorOpt *BehaviorOptions) (Conn, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}

	dc, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, err
	}

	err = dc.SetKeepAlive(true)
	if err != nil {
		return nil, err
	}

	c := &conn{
		behaviorOpt: behaviorOpt,
		Conn:        dc,
		reader:      protocol.NewReader(dc),
		timeout:     defaultTimeout,
	}

	return c, nil
}

// NewSimpleConn instantiates a new connection with the addr,
// which connects to the server and logs in with the userID and pwd.
func NewSimpleConn(ctx context.Context, address, userID, pwd string) (Conn, error) {
	conn, err := NewConn(ctx, address, nil)
	if err != nil {
		return nil, err
	}

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

// Add an init script which will be run after you call connect
// func (c *conn) AddInitScript(script string) {
// 	if c.initScripts == nil {
// 		c.initScripts = make([]string, 0)
// 	}
// 	c.initScripts = append(c.initScripts, script)
// }

func (c *conn) GetLocalAddress() string {
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

func (c *conn) RefreshTimeout(t time.Duration) {
	c.timeout = t
}

func (c *conn) Connect() error {
	h, _, err := c.run(&requestParams{
		commandType: connectCmd,
		Command:     generateConnectionCommand(),
	})
	if err != nil {
		return err
	}

	// if len(c.initScripts) != 0 {
	// 	for _, v := range c.initScripts {
	// 		_, err := c.RunScript(v)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// }

	c.connected = true
	c.refreshHeaderForResponse(h)

	return nil
}

func (c *conn) Close() error {
	if err := c.Conn.Close(); err != nil {
		return err
	}

	c.connected = false
	c.sessionID = nil

	return nil
}

func (c *conn) IsClosed() bool {
	return !c.connected
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
	if params.commandType == scriptCmd || params.commandType == functionCmd {
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
