package api

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/dolphindb/api-go/v3/model"
)

// DBConnectionPool is the client which helps you to handle tasks with connections.
type DBConnectionPool struct {
	mu sync.RWMutex

	isLoadBalance bool
	isClosed      bool

	loadBalanceAddresses []string

	opt           PoolOption
	connections   chan pooledConn
	timeout       time.Duration
	generation    uint64
	currentLeader string
	logName       string
}

type loadBalanceDiscoveryConn interface {
	RunScript(s string) (model.DataForm, error)
	Close() error
}

type pooledConn struct {
	conn       dialer.Conn
	generation uint64
}

// dialerNewConn is a test seam for stubbing dialer.NewConn in unit tests.
var dialerNewConn = dialer.NewConn
var openLoadBalanceDiscoveryConn = func(opt *PoolOption) (loadBalanceDiscoveryConn, error) {
	return newConn(opt.Address, opt)
}
var runPoolTask = func(d *DBConnectionPool, conn dialer.Conn, task *Task) (model.DataForm, *dialer.ExecutionTrace, error) {
	return d.RunTaskWithTrace(conn, task)
}

var (
	errConnectionPoolClosed = errors.New("connection pool is closed")
	errNilTask              = errors.New("task must not be nil")
	nextPoolID              atomic.Uint64
)

type logNamedConn interface {
	SetLogName(name string)
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

	// TryReconnectNums specifies the number of reconnection attempts.
	// Nil means reconnect indefinitely; any non-positive value is invalid.
	TryReconnectNums *int

	// EnableScram specifies whether SCRAM login is required.
	// When false, login still tries SCRAM first and falls back to password login
	// if the server or user does not support SCRAM.
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
		opt:                  clonePoolOption(opt),
		timeout:              timeout,
		generation:           1,
		logName:              nextPoolLogName(),
	}
	p.opt.Timeout = timeout

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
		p.connections = make(chan pooledConn, opt.PoolSize)
		for i := 0; i < opt.PoolSize; i++ {
			db, err := p.newConn(opt.Address, opt)
			if err != nil {
				p.apiLogErrorf("failed to instantiate a simple connection: %v", err)
				return nil, err
			}

			p.connections <- pooledConn{conn: db, generation: p.generation}
		}
	} else {
		err := p.initLoadBalanceConnections(opt)
		if err != nil {
			p.apiLogErrorf("failed to instantiate load-balance connections: %v", err)
			return nil, err
		}
	}

	return p, nil
}

func newConn(addr string, opt *PoolOption) (dialer.Conn, error) {
	return newNamedConn(addr, opt, "")
}

func (d *DBConnectionPool) newConn(addr string, opt *PoolOption) (dialer.Conn, error) {
	return newNamedConn(addr, opt, d.logName)
}

func newNamedConn(addr string, opt *PoolOption, logName string) (dialer.Conn, error) {
	bOpt := buildPoolBehaviorOptions(opt)
	conn, err := dialerNewConn(context.TODO(), addr, bOpt)
	if err != nil {
		apiLogErrorf("failed to instantiate a connection: %v", err)
		return nil, err
	}
	setConnLogName(conn, logName)

	conn.SetUserID(opt.UserID)
	conn.SetPassword(opt.Password)

	err = conn.Connect()
	if err != nil {
		apiLogErrorf("failed to connect to the server: %v", err)
		return nil, err
	}

	return conn, nil
}

func nextPoolLogName() string {
	return fmt.Sprintf("DDB-Pool-%d", nextPoolID.Add(1))
}

func setConnLogName(conn dialer.Conn, name string) {
	if name == "" {
		return
	}
	if named, ok := conn.(logNamedConn); ok {
		named.SetLogName(name)
	}
}

func buildPoolBehaviorOptions(opt *PoolOption) *dialer.BehaviorOptions {
	return &dialer.BehaviorOptions{
		Timeout:                opt.Timeout,
		NetTimeout:             opt.NetTimeout,
		EnableHighAvailability: opt.EnableHighAvailability,
		HighAvailabilitySites:  opt.HighAvailabilitySites,
		Reconnect:              opt.Reconnect,
		TryReconnectNums:       opt.TryReconnectNums,
		EnableScram:            opt.EnableScram,
		SqlStd:                 opt.SqlStd,
	}
}

func clonePoolOption(opt *PoolOption) PoolOption {
	if opt == nil {
		return PoolOption{}
	}

	cloned := *opt
	cloned.HighAvailabilitySites = slices.Clone(opt.HighAvailabilitySites)
	cloned.LoadBalanceAddresses = slices.Clone(opt.LoadBalanceAddresses)
	if opt.TryReconnectNums != nil {
		retries := *opt.TryReconnectNums
		cloned.TryReconnectNums = &retries
	}

	return cloned
}

func closeDialerConns(conns []dialer.Conn) error {
	var err error
	for _, conn := range conns {
		if conn != nil {
			err = errors.Join(err, conn.Close())
		}
	}

	return err
}

func (d *DBConnectionPool) RefreshTimeout(t time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.timeout = t
}

// Execute executes all task by connections with DBConnectionPool.
func (d *DBConnectionPool) Execute(tasks []*Task) error {
	wg := sync.WaitGroup{}
	errCh := make(chan error, len(tasks))
	for _, v := range tasks {
		if v == nil {
			continue
		}

		wg.Add(1)
		go func(task *Task) {
			defer wg.Done()
			if err := d.executeTask(task); err != nil {
				errCh <- err
			}
		}(v)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

// ExecuteTask executes one task by a connection in DBConnectionPool.
func (d *DBConnectionPool) ExecuteTask(task *Task) error {
	if task == nil {
		return errNilTask
	}

	if err := d.executeTask(task); err != nil {
		return err
	}

	return task.err
}

func (d *DBConnectionPool) executeTask(task *Task) error {
	task.result, task.err = nil, nil
	return d.executeTaskWithPoolFailover(task)
}

func (d *DBConnectionPool) executeTaskWithPoolFailover(task *Task) error {
	// First attempt: borrow a connection but do NOT return it yet.
	// If NotLeader is detected we pass it to switchPoolConnectionsToLeader
	// for reuse instead of creating a new connection for it.
	pc, err := d.borrowConn()
	if err != nil {
		task.result = nil
		task.err = err
		return err
	}

	var trace *dialer.ExecutionTrace
	task.result, trace, task.err = runPoolTask(d, pc.conn, task)
	target, hasTarget := poolSwitchTarget(trace, task.err)
	if !hasTarget {
		if task.err != nil {
			d.apiLogWarnf("task failed without NotLeader redirect: script=%q err=%v", task.Script, task.err)
		}
		task.err = errors.Join(task.err, d.returnConn(pc))
		if task.err == nil {
			return nil
		}
		return task.err
	}

	d.logPoolFailovers(trace)
	d.apiLogInfof("pool failover detected: target=%s taskErr=%v failovers=%d", target, task.err, len(trace.Failovers))

	if task.err != nil {
		firstErr := task.err
		d.apiLogInfof("task failed after conn-level retry (err=%v), retrying once before pool switch", firstErr)
		task.result, trace, task.err = runPoolTask(d, pc.conn, task)
		retryTarget, retryHasTarget := poolSwitchTarget(trace, task.err)
		if retryHasTarget {
			d.logPoolFailovers(trace)
			target = retryTarget
			d.apiLogInfof("pool failover retry detected: target=%s taskErr=%v failovers=%d", target, task.err, len(trace.Failovers))
		} else if task.err != nil {
			d.apiLogWarnf("retry on candidate connection failed without NotLeader redirect: script=%q err=%v", task.Script, task.err)
		}
	}

	taskErr := task.err
	switchErr := d.switchPoolConnectionsToLeader(target, pc)
	if switchErr != nil {
		// Switch failed. Return the borrowed connection to the (unchanged) pool.
		d.apiLogWarnf("pool switch to %s failed: %v (taskErr=%v)", target, switchErr, taskErr)
		task.err = errors.Join(taskErr, d.returnConn(pc))
		if taskErr == nil {
			return nil
		}
		task.err = taskErr
		return task.err
	}
	// Switch succeeded — pc was consumed (recycled into the new channel).
	d.apiLogInfof("pool switched to leader %s", target)
	if taskErr == nil {
		d.apiLogInfof("task already succeeded via conn-level retry, data written to %s", target)
		return nil
	}
	return taskErr
}

func (d *DBConnectionPool) logPoolFailovers(trace *dialer.ExecutionTrace) {
	if trace == nil {
		return
	}
	for i, f := range trace.Failovers {
		d.apiLogDebugf("failover[%d]: reason=%s from=%s to=%s err=%s", i, f.Reason, f.From, f.To, f.Err)
	}
}

func (d *DBConnectionPool) borrowConn() (pooledConn, error) {
	for {
		d.mu.RLock()
		if d.isClosed || d.connections == nil {
			d.mu.RUnlock()
			return pooledConn{}, errConnectionPoolClosed
		}
		connections := d.connections
		timeout := d.timeout
		d.mu.RUnlock()

		pc, ok := <-connections
		if !ok || pc.conn == nil {
			d.mu.RLock()
			retry := !d.isClosed && d.connections != nil && d.connections != connections
			d.mu.RUnlock()
			if retry {
				continue
			}
			return pooledConn{}, errConnectionPoolClosed
		}

		d.mu.RLock()
		stale := d.isClosed || d.connections != connections || pc.generation != d.generation
		d.mu.RUnlock()
		if stale {
			_ = pc.conn.Close()
			continue
		}

		pc.conn.RefreshTimeout(timeout)
		return pc, nil
	}
}

func (d *DBConnectionPool) returnConn(pc pooledConn) error {
	if pc.conn == nil {
		return nil
	}

	d.mu.Lock()
	if d.isClosed || d.connections == nil || pc.generation != d.generation {
		d.mu.Unlock()
		return pc.conn.Close()
	}
	d.connections <- pc
	d.mu.Unlock()

	return nil
}

func (d *DBConnectionPool) RunTask(conn dialer.Conn, task *Task) (model.DataForm, error) {
	df, _, err := d.RunTaskWithTrace(conn, task)
	return df, err
}

func (d *DBConnectionPool) RunTaskWithTrace(conn dialer.Conn, task *Task) (model.DataForm, *dialer.ExecutionTrace, error) {
	if task.Args != nil {
		return conn.RunFuncWithTrace(task.Script, task.Args)
	}

	return conn.RunScriptWithTrace(task.Script)
}

func (d *DBConnectionPool) switchPoolConnectionsToLeader(addr string, recycled pooledConn) error {
	if addr == "" {
		return nil
	}

	d.mu.RLock()
	if d.isClosed || d.connections == nil {
		d.mu.RUnlock()
		return errConnectionPoolClosed
	}
	if d.currentLeader == addr {
		d.mu.RUnlock()
		_ = d.returnConn(recycled)
		return nil
	}
	poolSize := cap(d.connections)
	opt := clonePoolOption(&d.opt)
	d.mu.RUnlock()

	opt.Address = addr
	opt.LoadBalance = false
	opt.LoadBalanceAddresses = nil
	opt.PoolSize = poolSize

	// Create poolSize-1 new connections; the recycled connection is reused below.
	conns := make([]dialer.Conn, 0, poolSize-1)
	for i := 1; i < poolSize; i++ {
		conn, err := d.newConn(addr, &opt)
		if err != nil {
			return errors.Join(err, closeDialerConns(conns))
		}
		conns = append(conns, conn)
	}

	d.mu.Lock()
	if d.isClosed || d.connections == nil {
		d.mu.Unlock()
		return errors.Join(errConnectionPoolClosed, closeDialerConns(conns))
	}
	if d.currentLeader == addr {
		d.mu.Unlock()
		return closeDialerConns(conns)
	}

	oldConnections := d.connections
	d.generation++
	newGeneration := d.generation
	newConnections := make(chan pooledConn, poolSize)
	// Put the recycled connection into the new channel first.
	setConnLogName(recycled.conn, d.logName)
	newConnections <- pooledConn{conn: recycled.conn, generation: newGeneration}
	for _, conn := range conns {
		newConnections <- pooledConn{conn: conn, generation: newGeneration}
	}
	d.connections = newConnections
	d.currentLeader = addr
	d.mu.Unlock()

	close(oldConnections)
	var err error
	for pc := range oldConnections {
		err = errors.Join(err, pc.conn.Close())
	}

	return err
}

func notLeaderTarget(trace *dialer.ExecutionTrace) (string, bool) {
	if trace == nil {
		return "", false
	}

	for i := len(trace.Failovers) - 1; i >= 0; i-- {
		failover := trace.Failovers[i]
		if failover.Reason == dialer.FailoverReasonNotLeader && failover.To != "" {
			return failover.To, true
		}
	}

	return "", false
}

func poolSwitchTarget(trace *dialer.ExecutionTrace, taskErr error) (string, bool) {
	if trace == nil {
		return "", false
	}
	if taskErr != nil {
		return notLeaderTarget(trace)
	}

	if _, ok := notLeaderTarget(trace); !ok {
		return "", false
	}

	for i := len(trace.Failovers) - 1; i >= 0; i-- {
		if trace.Failovers[i].To != "" {
			return trace.Failovers[i].To, true
		}
	}

	return "", false
}

// GetPoolSize return the size of DBConnectionPool.
func (d *DBConnectionPool) GetPoolSize() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.connections == nil {
		return 0
	}
	return len(d.connections)
}

// Close closes all connections in DBConnectionPool.
func (d *DBConnectionPool) Close() error {
	d.mu.Lock()
	if d.isClosed {
		d.mu.Unlock()
		return nil
	}

	d.isClosed = true
	connections := d.connections
	d.connections = nil
	d.mu.Unlock()

	if connections == nil {
		return nil
	}

	close(connections)

	var err error
	for v := range connections {
		err = errors.Join(err, v.conn.Close())
	}

	return err
}

// IsClosed checks whether the DBConnectionPool is closed.
func (d *DBConnectionPool) IsClosed() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.isClosed
}

func (d *DBConnectionPool) initLoadBalanceConnections(opt *PoolOption) error {
	addresses, err := d.resolveLoadBalanceAddresses(opt)
	if err != nil {
		return err
	}
	connOpt := d.loadBalanceConnOption(opt, addresses)
	d.opt = clonePoolOption(connOpt)

	d.connections = make(chan pooledConn, opt.PoolSize)
	for i := 0; i < opt.PoolSize; i++ {
		conn, err := d.newConn(addresses[i%len(addresses)], connOpt)
		if err != nil {
			d.apiLogErrorf("failed to instantiate a simple connection: %v", err)
			return err
		}

		d.connections <- pooledConn{conn: conn, generation: d.generation}
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
		copied.HighAvailabilitySites = slices.Clone(addresses)
	} else {
		copied.HighAvailabilitySites = slices.Clone(opt.HighAvailabilitySites)
	}

	return &copied
}

func (d *DBConnectionPool) getLoadBalanceAddress(opt *PoolOption) (address []string, err error) {
	db, err := openLoadBalanceDiscoveryConn(opt)
	if err != nil {
		d.apiLogErrorf("failed to instantiate a simple connection: %v", err)
		return nil, err
	}

	defer func() {
		err = errors.Join(err, db.Close())
	}()

	df, err := db.RunScript("rpc(getControllerAlias(), getClusterLiveDataNodes{false})")
	if err != nil {
		d.apiLogErrorf("failed to get nodes: %v", err)
		return nil, err
	}

	vct := df.(*model.Vector)
	nodes := vct.Data.StringList()
	address = make([]string, len(nodes))
	for k, v := range nodes {
		address[k], err = parseLoadBalanceNodeAddress(v)
		if err != nil {
			return nil, err
		}
	}

	if len(address) == 0 {
		return nil, errors.New("no available data nodes found in the cluster")
	}

	return address, nil
}

func parseLoadBalanceNodeAddress(raw string) (string, error) {
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
