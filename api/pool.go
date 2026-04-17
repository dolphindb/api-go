package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/dolphindb/api-go/v3/model"
)

// DBConnectionPool is the client which helps you to handle tasks with connections.
type DBConnectionPool struct {
	isLoadBalance bool
	isClosed      bool

	loadBalanceAddresses []string

	connections chan dialer.Conn
	timeout     time.Duration
}

type loadBalanceDiscoveryConn interface {
	RunScript(s string) (model.DataForm, error)
	Close() error
}

// dialerNewConn is a test seam for stubbing dialer.NewConn in unit tests.
var dialerNewConn = dialer.NewConn
var openLoadBalanceDiscoveryConn = func(opt *PoolOption) (loadBalanceDiscoveryConn, error) {
	return newConn(opt.Address, opt)
}
var runPoolTask = func(d *DBConnectionPool, conn dialer.Conn, task *Task) (model.DataForm, error) {
	conn.RefreshTimeout(d.timeout)
	return d.RunTask(conn, task)
}

// PoolOption helps you to configure DBConnectionPool by calling NewDBConnectionPool.
type PoolOption struct {
	// the server address
	Address string
	// the user ID
	UserID string
	// password of the user
	Password string
	// the size of connection pool
	// only takes effect when LoadBalance is false
	PoolSize int
	// Whether to enable load balancing.
	// If true and LoadBalanceAddresses is empty, the pool uses Address together with
	// HighAvailabilitySites when HighAvailabilitySites is configured; otherwise it
	// discovers available data nodes and creates connections evenly across them.
	// If LoadBalanceAddresses is set, the pool creates connections evenly across Address,
	// LoadBalanceAddresses, and HighAvailabilitySites after deduplication.
	LoadBalance bool

	// Whether to enable high availability.
	// If true, when the address is unrearched, another address in HighAvailabilitySites will be connected.
	EnableHighAvailability bool

	// Available only if EnableHighAvailability is true.
	// When LoadBalance is true, these sites are merged into the candidate node list.
	// If LoadBalanceAddresses is empty, specifying these sites also bypasses
	// load-balance node discovery.
	HighAvailabilitySites []string

	// addresses of load balance
	// When LoadBalance is true, these addresses are combined with Address and
	// HighAvailabilitySites, then deduplicated.
	LoadBalanceAddresses []string

	// how long each request waits for the server response
	Timeout time.Duration
	// network-layer timeout budget used for TCP connect, Linux
	// TCP_USER_TIMEOUT and keepalive probing
	NetTimeout time.Duration

	// whether to reconnect if not enable high availability
	Reconnect bool

	// try reconnect times
	TryReconnectNums *int

	// if enable SCRAM login verify
	EnableScram bool

	// SqlStd specifies which SQL standard should be used for the session.
	SqlStd dialer.SqlStdEnum
}

// NewDBConnectionPool inits a DBConnectionPool object and configures it with opt, finally returns it.
func NewDBConnectionPool(opt *PoolOption) (*DBConnectionPool, error) {
	timeout := time.Minute
	if opt.Timeout != 0 {
		if opt.Timeout < 0 {
			return nil, errors.New("Timeout must be equal or greater than 0")
		}
		timeout = opt.Timeout
	}
	p := &DBConnectionPool{
		isLoadBalance:        opt.LoadBalance,
		loadBalanceAddresses: opt.LoadBalanceAddresses,
		timeout:              timeout,
	}

	if opt.PoolSize < 1 {
		return nil, errors.New("PoolSize must be greater than 0")
	}
	if !opt.LoadBalance && !opt.EnableHighAvailability && len(opt.HighAvailabilitySites) > 0 {
		return nil, errors.New("HighAvailabilitySites requires LoadBalance or EnableHighAvailability to be true")
	}
	if !opt.LoadBalance && len(opt.LoadBalanceAddresses) > 0 {
		return nil, errors.New("LoadBalanceAddresses requires LoadBalance to be true")
	}

	if !opt.LoadBalance {
		p.connections = make(chan dialer.Conn, opt.PoolSize)
		for i := 0; i < opt.PoolSize; i++ {
			db, err := newConn(opt.Address, opt)
			if err != nil {
				fmt.Printf("Failed to instantiate a simple connection: %s\n", err.Error())
				return nil, err
			}

			p.connections <- db
		}
	} else {
		err := p.initLoadBalanceConnections(opt)
		if err != nil {
			fmt.Printf("Failed to instantiate loadBalance connections: %s\n", err.Error())
			return nil, err
		}
	}

	return p, nil
}

func newConn(addr string, opt *PoolOption) (dialer.Conn, error) {
	bOpt := &dialer.BehaviorOptions{
		EnableHighAvailability: opt.EnableHighAvailability,
		HighAvailabilitySites:  opt.HighAvailabilitySites,
		Timeout:                opt.Timeout,
		NetTimeout:             opt.NetTimeout,
		Reconnect:              opt.Reconnect,
		TryReconnectNums:       opt.TryReconnectNums,
		EnableScram:            opt.EnableScram,
		SqlStd:                 opt.SqlStd,
	}
	conn, err := dialerNewConn(context.TODO(), addr, bOpt)
	if err != nil {
		fmt.Printf("Failed to instantiate a connection: %s\n", err.Error())
		return nil, err
	}

	conn.SetUserID(opt.UserID)
	conn.SetPassword(opt.Password)

	err = conn.Connect()
	if err != nil {
		fmt.Printf("Failed to connect to the server: %s\n", err.Error())
		return nil, err
	}

	return conn, nil
}

func (d *DBConnectionPool) RefreshTimeout(t time.Duration) {
	d.timeout = t
}

// Execute executes all task by connections with DBConnectionPool.
func (d *DBConnectionPool) Execute(tasks []*Task) error {
	wg := sync.WaitGroup{}
	for _, v := range tasks {
		if v == nil {
			continue
		}

		wg.Add(1)
		go func(task *Task) {
			defer wg.Done()
			d.executeTask(task)
		}(v)
	}

	wg.Wait()

	return nil
}

// ExecuteTask executes one task by a connection in DBConnectionPool.
func (d *DBConnectionPool) ExecuteTask(task *Task) error {
	if task == nil {
		return errors.New("task must not be nil")
	}

	d.executeTask(task)
	return task.err
}

func (d *DBConnectionPool) executeTask(task *Task) {
	conn := <-d.connections
	defer func() {
		d.connections <- conn
	}()

	task.result, task.err = runPoolTask(d, conn, task)
}

func (d *DBConnectionPool) RunTask(conn dialer.Conn, task *Task) (model.DataForm, error) {
	if task.Args != nil {
		return conn.RunFunc(task.Script, task.Args)
	}

	return conn.RunScript(task.Script)
}

// GetPoolSize return the size of DBConnectionPool.
func (d *DBConnectionPool) GetPoolSize() int {
	return len(d.connections)
}

// Close closes all connections in DBConnectionPool.
func (d *DBConnectionPool) Close() error {
	if d.isClosed {
		return nil
	}

	close(d.connections)

	for v := range d.connections {
		err := v.Close()
		if err != nil {
			return err
		}
	}

	d.isClosed = true

	return nil
}

// IsClosed checks whether the DBConnectionPool is closed.
func (d *DBConnectionPool) IsClosed() bool {
	return d.isClosed
}

func (d *DBConnectionPool) initLoadBalanceConnections(opt *PoolOption) error {
	addresses, err := d.resolveLoadBalanceAddresses(opt)
	if err != nil {
		return err
	}
	connOpt := d.loadBalanceConnOption(opt, addresses)

	d.connections = make(chan dialer.Conn, opt.PoolSize)
	for i := 0; i < opt.PoolSize; i++ {
		conn, err := newConn(addresses[i%len(addresses)], connOpt)
		if err != nil {
			fmt.Printf("Failed to instantiate a simple connection: %s\n", err.Error())
			return err
		}

		d.connections <- conn
	}

	return nil
}

func (d *DBConnectionPool) resolveLoadBalanceAddresses(opt *PoolOption) ([]string, error) {
	var addresses []string
	if len(d.loadBalanceAddresses) > 0 {
		addresses = append(addresses, opt.Address)
		addresses = append(addresses, d.loadBalanceAddresses...)
		addresses = append(addresses, opt.HighAvailabilitySites...)
	} else if len(opt.HighAvailabilitySites) > 0 {
		addresses = append(addresses, opt.Address)
		addresses = append(addresses, opt.HighAvailabilitySites...)
	} else {
		discovered, err := d.getLoadBalanceAddress(opt)
		if err != nil {
			return nil, err
		}
		addresses = append(addresses, discovered...)
	}

	addresses = deduplicateAddresses(addresses)
	if len(addresses) == 0 {
		return nil, errors.New("no available load balance addresses configured")
	}

	return addresses, nil
}

func (d *DBConnectionPool) loadBalanceConnOption(opt *PoolOption, addresses []string) *PoolOption {
	copied := *opt
	if opt.EnableHighAvailability {
		copied.HighAvailabilitySites = append([]string(nil), addresses...)
	} else {
		copied.HighAvailabilitySites = append([]string(nil), opt.HighAvailabilitySites...)
	}

	return &copied
}

func (d *DBConnectionPool) getLoadBalanceAddress(opt *PoolOption) ([]string, error) {
	db, err := openLoadBalanceDiscoveryConn(opt)
	if err != nil {
		fmt.Printf("Failed to instantiate a simple connection: %s\n", err.Error())
		return nil, err
	}

	defer db.Close()

	df, err := db.RunScript("rpc(getControllerAlias(), getClusterLiveDataNodes{false})")
	if err != nil {
		fmt.Printf("Failed to get nodes: %s\n", err.Error())
		return nil, err
	}

	vct := df.(*model.Vector)
	nodes := vct.Data.StringList()
	address := make([]string, len(nodes))
	for k, v := range nodes {
		fields := strings.Split(v, ":")
		if len(fields) < 2 {
			return nil, errors.New("invalid data node address: " + v)
		}

		address[k] = fmt.Sprintf("%s:%s", fields[0], fields[1])
	}

	if len(address) == 0 {
		return nil, errors.New("no available data nodes found in the cluster")
	}

	return address, nil
}

func deduplicateAddresses(addresses []string) []string {
	seen := make(map[string]struct{}, len(addresses))
	deduplicated := make([]string, 0, len(addresses))
	for _, address := range addresses {
		if address == "" {
			continue
		}
		if _, ok := seen[address]; ok {
			continue
		}

		seen[address] = struct{}{}
		deduplicated = append(deduplicated, address)
	}

	return deduplicated
}
