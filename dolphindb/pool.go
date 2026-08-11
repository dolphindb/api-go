package dolphindb

import (
	"context"
	"errors"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dolphindb/api-go/v3/api"
	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/dolphindb/api-go/v3/model"
)

// ConnPool is the preferred explicit borrow/return connection pool.
type ConnPool struct {
	mu sync.Mutex

	isClosed bool
	timeout  time.Duration

	connections chan dialer.Conn
	borrowed    map[dialer.Conn]struct{}
}

// ConnLease represents a borrowed connection. Close returns it to the pool.
type ConnLease struct {
	pool *ConnPool
	conn dialer.Conn
	once sync.Once
	err  error
}

// NewConnPool creates a connection pool for explicit session borrowing.
func NewConnPool(opts *PoolOptions) (*ConnPool, error) {
	normalized, timeout, err := normalizePoolOptions(opts)
	if err != nil {
		return nil, err
	}

	pool := &ConnPool{
		timeout:     timeout,
		connections: make(chan dialer.Conn, normalized.PoolSize),
		borrowed:    make(map[dialer.Conn]struct{}, normalized.PoolSize),
	}

	addresses, connOpt, err := resolveConnPoolAddresses(normalized)
	if err != nil {
		return nil, err
	}

	for i := 0; i < normalized.PoolSize; i++ {
		conn, err := newPoolConn(addresses[i%len(addresses)], connOpt)
		if err != nil {
			_ = pool.Close()
			return nil, err
		}
		pool.connections <- conn
	}

	return pool, nil
}

// TaskPool is the task-oriented pooled executor formerly exposed as DBConnectionPool.
type TaskPool struct {
	*api.DBConnectionPool
}

// NewTaskPool creates a task-oriented pooled executor.
func NewTaskPool(opts *PoolOptions) (*TaskPool, error) {
	pool, err := api.NewDBConnectionPool(opts)
	if err != nil {
		return nil, err
	}

	return &TaskPool{DBConnectionPool: pool}, nil
}

// NewTableAppender creates a table appender from the provided options.
func NewTableAppender(opts *TableAppenderOptions) (*TableAppender, error) {
	return api.NewTableAppender(opts)
}

// NewPartitionedTableAppender creates a partitioned table appender from the provided options.
func NewPartitionedTableAppender(opts *PartitionedTableAppenderOptions) (*PartitionedTableAppender, error) {
	return api.NewPartitionedTableAppender(opts)
}

// Acquire borrows a connected session from the pool.
// The caller should return it by calling Close on the returned lease.
func (p *ConnPool) Acquire() (*ConnLease, error) {
	if p == nil {
		return nil, errors.New("connection pool is closed")
	}

	p.mu.Lock()
	if p.isClosed || p.connections == nil {
		p.mu.Unlock()
		return nil, errors.New("connection pool is closed")
	}
	connections := p.connections
	timeout := p.timeout
	p.mu.Unlock()

	conn, ok := <-connections
	if !ok || conn == nil {
		return nil, errors.New("connection pool is closed")
	}

	conn.RefreshTimeout(timeout)

	p.mu.Lock()
	if p.isClosed || p.connections != connections {
		p.mu.Unlock()
		return nil, errors.Join(errors.New("connection pool is closed"), conn.Close())
	}
	p.borrowed[conn] = struct{}{}
	p.mu.Unlock()

	return &ConnLease{pool: p, conn: conn}, nil
}

// Release returns a borrowed connection to the pool.
func (p *ConnPool) Release(conn dialer.Conn) error {
	if p == nil {
		if conn == nil {
			return nil
		}
		return conn.Close()
	}

	if conn == nil {
		return nil
	}

	p.mu.Lock()
	if _, ok := p.borrowed[conn]; !ok {
		p.mu.Unlock()
		return errors.New("connection was not borrowed from this pool")
	}
	delete(p.borrowed, conn)

	if p.isClosed || p.connections == nil {
		p.mu.Unlock()
		return conn.Close()
	}

	p.connections <- conn
	p.mu.Unlock()
	return nil
}

// WithConn borrows a connection, executes fn, then returns the connection to the pool.
func (p *ConnPool) WithConn(fn func(dialer.Conn) error) (err error) {
	if fn == nil {
		return errors.New("fn must not be nil")
	}

	lease, err := p.Acquire()
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, lease.Close())
	}()

	return fn(lease.Conn())
}

// Size returns the number of currently idle connections in the pool.
func (p *ConnPool) Size() int {
	if p == nil {
		return 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.connections == nil {
		return 0
	}

	return len(p.connections)
}

// RefreshTimeout updates the timeout applied to newly borrowed connections.
func (p *ConnPool) RefreshTimeout(t time.Duration) {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.timeout = t
}

// Close closes the pool and all idle connections.
func (p *ConnPool) Close() error {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	if p.isClosed {
		p.mu.Unlock()
		return nil
	}

	p.isClosed = true
	connections := p.connections
	p.connections = nil
	p.mu.Unlock()

	if connections == nil {
		return nil
	}

	close(connections)

	var err error
	for conn := range connections {
		err = errors.Join(err, conn.Close())
	}

	return err
}

// IsClosed reports whether the pool has been closed.
func (p *ConnPool) IsClosed() bool {
	if p == nil {
		return true
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	return p.isClosed
}

// Conn returns the borrowed connection.
func (l *ConnLease) Conn() dialer.Conn {
	if l == nil {
		return nil
	}

	return l.conn
}

// Close returns the borrowed connection to the pool. It is safe to call more
// than once; repeated calls return the first close result.
func (l *ConnLease) Close() error {
	if l == nil {
		return nil
	}

	l.once.Do(func() {
		if l.pool == nil {
			if l.conn != nil {
				l.err = l.conn.Close()
			}
			return
		}

		l.err = l.pool.Release(l.conn)
	})

	return l.err
}

func normalizePoolOptions(opts *PoolOptions) (*PoolOptions, time.Duration, error) {
	if opts == nil {
		return nil, 0, errors.New("PoolOptions must not be nil")
	}

	timeout := time.Minute
	if opts.Timeout != 0 {
		if opts.Timeout < 0 {
			return nil, 0, errors.New("Timeout must be equal or greater than 0")
		}
		timeout = opts.Timeout
	}
	if opts.PoolSize < 1 {
		return nil, 0, errors.New("PoolSize must be greater than 0")
	}
	if !opts.LoadBalance && !opts.EnableHighAvailability && len(opts.HighAvailabilitySites) > 0 {
		return nil, 0, errors.New("HighAvailabilitySites requires LoadBalance or EnableHighAvailability to be true")
	}
	if !opts.LoadBalance && len(opts.LoadBalanceAddresses) > 0 {
		return nil, 0, errors.New("LoadBalanceAddresses requires LoadBalance to be true")
	}
	if opts.TryReconnectNums != nil && *opts.TryReconnectNums <= 0 {
		return nil, 0, errors.New("TryReconnectNums must be nil or greater than 0")
	}

	normalized := *opts
	normalized.HighAvailabilitySites = slices.Clone(opts.HighAvailabilitySites)
	normalized.LoadBalanceAddresses = slices.Clone(opts.LoadBalanceAddresses)
	normalized.Timeout = timeout
	return &normalized, timeout, nil
}

func resolveConnPoolAddresses(opts *PoolOptions) ([]string, *PoolOptions, error) {
	if !opts.LoadBalance {
		return []string{opts.Address}, opts, nil
	}

	addresses, err := connPoolLoadBalanceAddresses(opts)
	if err != nil {
		return nil, nil, err
	}

	connOpt := *opts
	if opts.EnableHighAvailability {
		connOpt.HighAvailabilitySites = slices.Clone(addresses)
	} else {
		connOpt.HighAvailabilitySites = nil
	}

	return addresses, &connOpt, nil
}

func connPoolLoadBalanceAddresses(opts *PoolOptions) ([]string, error) {
	var addresses []string
	if len(opts.LoadBalanceAddresses) > 0 {
		addresses = append(addresses, opts.Address)
		addresses = append(addresses, opts.LoadBalanceAddresses...)
		addresses = append(addresses, opts.HighAvailabilitySites...)
	} else if len(opts.HighAvailabilitySites) > 0 {
		addresses = append(addresses, opts.Address)
		addresses = append(addresses, opts.HighAvailabilitySites...)
	} else {
		discovered, err := discoverConnPoolLoadBalanceAddresses(opts)
		if err != nil {
			return nil, err
		}
		addresses = append(addresses, discovered...)
	}

	addresses = deduplicatePoolAddresses(addresses)
	if len(addresses) == 0 {
		return nil, errors.New("no available load balance addresses configured")
	}

	return addresses, nil
}

func discoverConnPoolLoadBalanceAddresses(opts *PoolOptions) (addresses []string, err error) {
	conn, err := newPoolConn(opts.Address, opts)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, conn.Close())
	}()

	df, err := conn.RunScript("rpc(getControllerAlias(), getClusterLiveDataNodes{false})")
	if err != nil {
		return nil, err
	}

	vct := df.(*model.Vector)
	nodes := vct.Data.StringList()
	addresses = make([]string, len(nodes))
	for i, node := range nodes {
		addresses[i], err = parsePoolLoadBalanceNodeAddress(node)
		if err != nil {
			return nil, err
		}
	}

	if len(addresses) == 0 {
		return nil, errors.New("no available data nodes found in the cluster")
	}

	return addresses, nil
}

func newPoolConn(addr string, opts *PoolOptions) (dialer.Conn, error) {
	conn, err := dialer.NewConn(context.TODO(), addr, connPoolBehaviorOptions(opts))
	if err != nil {
		return nil, err
	}

	conn.SetUserID(opts.UserID)
	conn.SetPassword(opts.Password)
	if err := conn.Connect(); err != nil {
		return nil, err
	}

	return conn, nil
}

func connPoolBehaviorOptions(opts *PoolOptions) *dialer.BehaviorOptions {
	return &dialer.BehaviorOptions{
		Timeout:                opts.Timeout,
		NetTimeout:             opts.NetTimeout,
		EnableHighAvailability: opts.EnableHighAvailability,
		HighAvailabilitySites:  opts.HighAvailabilitySites,
		Reconnect:              opts.Reconnect,
		TryReconnectNums:       opts.TryReconnectNums,
		EnableScram:            opts.EnableScram,
		SqlStd:                 opts.SqlStd,
	}
}

func parsePoolLoadBalanceNodeAddress(raw string) (string, error) {
	lastColon := strings.LastIndex(raw, ":")
	if lastColon < 0 {
		return "", errors.New("invalid data node address: " + raw)
	}

	host, port, err := net.SplitHostPort(raw[:lastColon])
	if err != nil {
		return "", errors.New("invalid data node address: " + raw)
	}

	return net.JoinHostPort(host, port), nil
}

func deduplicatePoolAddresses(addresses []string) []string {
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
