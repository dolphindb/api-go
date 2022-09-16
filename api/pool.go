package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/dolphindb/api-go/dialer"
	"github.com/dolphindb/api-go/model"
)

// DBConnectionPool is the client which helps you to handle tasks with connections.
type DBConnectionPool struct {
	isLoadBalance bool
	isClosed      bool

	loadBalanceAddresses []string

	connections chan dialer.Conn
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
	// If true, getClusterLiveDataNodes will be called to get all available datanode addresses
	// and connection to every address will be created.
	// If the addresses are not available, you can set LoadBalanceAddresses instead.
	LoadBalance bool

	// addresses of load balance
	LoadBalanceAddresses []string
}

// NewDBConnectionPool inits a DBConnectionPool object and configures it with opt, finally returns it.
func NewDBConnectionPool(opt *PoolOption) (*DBConnectionPool, error) {
	p := &DBConnectionPool{
		isLoadBalance:        opt.LoadBalance,
		loadBalanceAddresses: opt.LoadBalanceAddresses,
	}

	if opt.PoolSize < 1 {
		return nil, errors.New("PoolSize must be greater than 0")
	}

	if !opt.LoadBalance {
		p.connections = make(chan dialer.Conn, opt.PoolSize)
		for i := 0; i < opt.PoolSize; i++ {
			db, err := dialer.NewSimpleConn(context.TODO(), opt.Address, opt.UserID, opt.Password)
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

// Execute executes all task by connections with DBConnectionPool.
func (d *DBConnectionPool) Execute(tasks []*Task) error {
	wg := sync.WaitGroup{}
	for _, v := range tasks {
		if v == nil {
			continue
		}

		wg.Add(1)
		if v.Args != nil {
			go func(task *Task) {
				conn := <-d.connections
				task.result, task.err = conn.RunFunc(task.Script, task.Args)
				d.connections <- conn
				wg.Done()
			}(v)
		} else {
			go func(task *Task) {
				conn := <-d.connections
				task.result, task.err = conn.RunScript(task.Script)
				d.connections <- conn
				wg.Done()
			}(v)
		}
	}

	wg.Wait()

	return nil
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
	var address []string
	var err error

	d.connections = make(chan dialer.Conn, opt.PoolSize)
	if len(d.loadBalanceAddresses) > 0 {
		address = d.loadBalanceAddresses
	} else {
		address, err = d.getLoadBalanceAddress(opt)
		if err != nil {
			return err
		}
	}

	for i := 0; i < opt.PoolSize; i++ {
		conn, err := dialer.NewSimpleConn(context.TODO(), address[i%len(address)], opt.UserID, opt.Password)
		if err != nil {
			fmt.Printf("Failed to instantiate a simple connection: %s\n", err.Error())
			return err
		}

		d.connections <- conn
	}

	return nil
}

func (d *DBConnectionPool) getLoadBalanceAddress(opt *PoolOption) ([]string, error) {
	db, err := dialer.NewSimpleConn(context.TODO(), opt.Address, opt.UserID, opt.Password)
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

	return address, nil
}
