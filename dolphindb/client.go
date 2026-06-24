package dolphindb

import (
	"context"
	"net"
	"time"

	"github.com/dolphindb/api-go/v3/api"
	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/dolphindb/api-go/v3/model"
)

type Client struct {
	conn api.DolphinDB
}

// NewClient creates a client without connecting it.
// Call Connect and Login explicitly when you want staged session setup.
func NewClient(ctx context.Context, addr string, opts *dialer.BehaviorOptions) (*Client, error) {
	conn, err := api.NewDolphinDBClient(ctx, addr, opts)
	if err != nil {
		return nil, err
	}

	return &Client{conn: conn}, nil
}

// Dial creates, connects, and logs in a client in one step.
func Dial(addr, userID, password string, opts *dialer.BehaviorOptions) (*Client, error) {
	conn, err := api.NewDolphinDBClient(context.Background(), addr, opts)
	if err != nil {
		return nil, err
	}
	conn.SetUserID(userID)
	conn.SetPassword(password)
	if err := conn.Connect(); err != nil {
		return nil, err
	}

	return &Client{conn: conn}, nil
}

func (c *Client) Read(p []byte) (int, error) {
	return c.conn.Read(p)
}

func (c *Client) Write(p []byte) (int, error) {
	return c.conn.Write(p)
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *Client) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Client) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *Client) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *Client) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

func (c *Client) Connect() error {
	return c.conn.Connect()
}

func (c *Client) GetLocalAddress() string {
	return c.conn.GetLocalAddress()
}

func (c *Client) RefreshTimeout(t time.Duration) {
	c.conn.RefreshTimeout(t)
}

func (c *Client) GetSession() string {
	return c.conn.GetSession()
}

func (c *Client) IsClosed() bool {
	return c.conn.IsClosed()
}

func (c *Client) IsConnected() bool {
	return c.conn.IsConnected()
}

func (c *Client) GetUserID() string {
	return c.conn.GetUserID()
}

func (c *Client) SetUserID(userID string) {
	c.conn.SetUserID(userID)
}

func (c *Client) GetPassword() string {
	return c.conn.GetPassword()
}

func (c *Client) SetPassword(password string) {
	c.conn.SetPassword(password)
}

func (c *Client) RunScript(script string) (model.DataForm, error) {
	return c.conn.RunScript(script)
}

func (c *Client) RunScriptWithTrace(script string) (model.DataForm, *dialer.ExecutionTrace, error) {
	return c.conn.RunScriptWithTrace(script)
}

func (c *Client) RunFile(path string) (model.DataForm, error) {
	return c.conn.RunFile(path)
}

func (c *Client) RunFunc(name string, args []model.DataForm) (model.DataForm, error) {
	return c.conn.RunFunc(name, args)
}

func (c *Client) RunFuncWithTrace(name string, args []model.DataForm) (model.DataForm, *dialer.ExecutionTrace, error) {
	return c.conn.RunFuncWithTrace(name, args)
}

func (c *Client) Upload(vars map[string]model.DataForm) (model.DataForm, error) {
	return c.conn.Upload(vars)
}

func (c *Client) GetTCPConn() *net.TCPConn {
	return c.conn.GetTCPConn()
}

func (c *Client) Login(req *LoginRequest) error {
	return c.conn.Login(req)
}

func (c *Client) Logout() error {
	return c.conn.Logout()
}
