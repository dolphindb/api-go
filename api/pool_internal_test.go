package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/dolphindb/api-go/v3/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeDiscoveryConn struct {
	df     model.DataForm
	closed bool
}

func (f *fakeDiscoveryConn) RunScript(string) (model.DataForm, error) { return f.df, nil }
func (f *fakeDiscoveryConn) Close() error {
	f.closed = true
	return nil
}

type fakePoolTaskConn struct {
	dialer.Conn
	addr      string
	userID    string
	password  string
	closed    bool
	connected bool
	timeout   time.Duration
	logName   string
}

func (f *fakePoolTaskConn) Connect() error {
	f.connected = true
	return nil
}

func (f *fakePoolTaskConn) Close() error {
	f.closed = true
	return nil
}

func (f *fakePoolTaskConn) SetUserID(userID string) {
	f.userID = userID
}

func (f *fakePoolTaskConn) SetPassword(password string) {
	f.password = password
}

func (f *fakePoolTaskConn) RefreshTimeout(t time.Duration) {
	f.timeout = t
}

func (f *fakePoolTaskConn) SetLogName(name string) {
	f.logName = name
}

func TestNewDBConnectionPoolAssignsPoolLogNameToConnections(t *testing.T) {
	originalDialerNewConn := dialerNewConn
	defer func() {
		dialerNewConn = originalDialerNewConn
	}()

	created := make([]*fakePoolTaskConn, 0, 2)
	dialerNewConn = func(_ context.Context, addr string, _ *dialer.BehaviorOptions) (dialer.Conn, error) {
		conn := &fakePoolTaskConn{addr: addr}
		created = append(created, conn)
		return conn, nil
	}

	pool, err := NewDBConnectionPool(&PoolOption{
		Address:  "127.0.0.1:8848",
		UserID:   "user",
		Password: "pwd",
		PoolSize: 2,
	})
	require.NoError(t, err)
	defer pool.Close()

	require.Len(t, created, 2)
	assert.Equal(t, pool.logName, created[0].logName)
	assert.Equal(t, pool.logName, created[1].logName)
	assert.Equal(t, "127.0.0.1:8848", pool.currentLeader)
	assert.True(t, created[0].connected)
	assert.True(t, created[1].connected)
}

func TestNewLoadBalancedPoolDoesNotClaimSingleInitialLeader(t *testing.T) {
	originalDialerNewConn := dialerNewConn
	defer func() { dialerNewConn = originalDialerNewConn }()

	dialerNewConn = func(_ context.Context, addr string, _ *dialer.BehaviorOptions) (dialer.Conn, error) {
		return &fakePoolTaskConn{addr: addr}, nil
	}
	pool, err := NewDBConnectionPool(&PoolOption{
		Address:              "node-a:8848",
		PoolSize:             2,
		LoadBalance:          true,
		LoadBalanceAddresses: []string{"node-b:8848"},
	})
	require.NoError(t, err)
	defer pool.Close()

	assert.Empty(t, pool.currentLeader)
}

func TestExecuteTaskRunsScriptAndReturnsConnectionToPool(t *testing.T) {
	original := runPoolTask
	defer func() {
		runPoolTask = original
	}()

	dt, err := model.NewDataType(model.DtString, "done")
	require.NoError(t, err)

	result := model.NewScalar(dt)
	conn, err := dialer.NewConn(context.TODO(), "127.0.0.1:0", nil)
	require.NoError(t, err)

	var gotTimeout time.Duration
	var gotScript string
	runPoolTask = func(d *DBConnectionPool, conn dialer.Conn, task *Task) (model.DataForm, *dialer.ExecutionTrace, error) {
		gotTimeout = d.timeout
		gotScript = task.Script
		return result, nil, nil
	}

	pool := &DBConnectionPool{
		connections: make(chan pooledConn, 1),
		timeout:     2 * time.Second,
		generation:  1,
	}
	pool.connections <- pooledConn{conn: conn, generation: 1}

	task := &Task{Script: "1..10"}
	err = pool.ExecuteTask(task)
	require.NoError(t, err)

	assert.Equal(t, "1..10", gotScript)
	assert.Equal(t, 2*time.Second, gotTimeout)
	assert.Same(t, result, task.GetResult())
	assert.True(t, task.IsSuccess())
	assert.Equal(t, 1, len(pool.connections))
}

func TestExecuteTaskSwitchesConnectionsToNotLeaderTarget(t *testing.T) {
	originalRunPoolTask := runPoolTask
	originalDialerNewConn := dialerNewConn
	defer func() {
		runPoolTask = originalRunPoolTask
		dialerNewConn = originalDialerNewConn
	}()

	old1 := &fakePoolTaskConn{addr: "old-1"}
	old2 := &fakePoolTaskConn{addr: "old-2"}
	created := make([]*fakePoolTaskConn, 0, 1)
	var recycledConn *fakePoolTaskConn
	dialerNewConn = func(_ context.Context, addr string, _ *dialer.BehaviorOptions) (dialer.Conn, error) {
		conn := &fakePoolTaskConn{addr: addr}
		created = append(created, conn)
		return conn, nil
	}

	runPoolTask = func(d *DBConnectionPool, conn dialer.Conn, task *Task) (model.DataForm, *dialer.ExecutionTrace, error) {
		recycledConn = conn.(*fakePoolTaskConn)
		return nil, &dialer.ExecutionTrace{
			Failovers: []dialer.FailoverTrace{{
				Reason: dialer.FailoverReasonNotLeader,
				From:   "old:8848",
				To:     "leader:8848",
			}},
		}, nil
	}

	pool := &DBConnectionPool{
		opt:         PoolOption{Address: "old:8848", UserID: "user", Password: "pwd", PoolSize: 2},
		connections: make(chan pooledConn, 2),
		timeout:     time.Second,
		generation:  1,
	}
	pool.connections <- pooledConn{conn: old1, generation: 1}
	pool.connections <- pooledConn{conn: old2, generation: 1}

	task := &Task{Script: "insert"}
	err := pool.ExecuteTask(task)
	require.NoError(t, err)

	// Only 1 new connection is created (poolSize-1); the recycled conn is reused.
	require.Len(t, created, 1)
	assert.Equal(t, "leader:8848", created[0].addr)
	assert.Equal(t, "user", created[0].userID)
	assert.Equal(t, "pwd", created[0].password)
	assert.True(t, created[0].connected)
	// The connection that triggered NotLeader is recycled, not closed.
	assert.False(t, recycledConn.closed)
	// The other connection in the old channel is drained and closed.
	assert.True(t, old2.closed)
	assert.Equal(t, uint64(2), pool.generation)
	assert.Equal(t, "leader:8848", pool.currentLeader)
	assert.Equal(t, 2, len(pool.connections))
}

func TestExecuteTaskKeepsPoolAfterSuccessfulHAFailoverWithoutNotLeader(t *testing.T) {
	originalRunPoolTask := runPoolTask
	originalDialerNewConn := dialerNewConn
	defer func() {
		runPoolTask = originalRunPoolTask
		dialerNewConn = originalDialerNewConn
	}()

	old1 := &fakePoolTaskConn{addr: "old-1"}
	old2 := &fakePoolTaskConn{addr: "old-2"}
	dialerNewConn = func(_ context.Context, addr string, _ *dialer.BehaviorOptions) (dialer.Conn, error) {
		t.Fatalf("pure HA success should not rebuild pool to %s", addr)
		return nil, nil
	}

	runPoolTask = func(d *DBConnectionPool, conn dialer.Conn, task *Task) (model.DataForm, *dialer.ExecutionTrace, error) {
		return nil, &dialer.ExecutionTrace{
			Failovers: []dialer.FailoverTrace{{
				Reason: dialer.FailoverReasonHighAvailability,
				From:   "old:8848",
				To:     "ha-target:8848",
			}},
		}, nil
	}

	pool := &DBConnectionPool{
		opt:         PoolOption{Address: "old:8848", UserID: "user", Password: "pwd", PoolSize: 2},
		connections: make(chan pooledConn, 2),
		timeout:     time.Second,
		generation:  1,
	}
	pool.connections <- pooledConn{conn: old1, generation: 1}
	pool.connections <- pooledConn{conn: old2, generation: 1}

	task := &Task{Script: "insert"}
	err := pool.ExecuteTask(task)
	require.NoError(t, err)
	assert.NoError(t, task.GetError())
	assert.Equal(t, uint64(1), pool.generation)
	assert.Empty(t, pool.currentLeader)
	assert.False(t, old1.closed)
	assert.False(t, old2.closed)
	assert.Equal(t, 2, len(pool.connections))
}

func TestExecuteTaskKeepsPoolAfterSameNodeConvergenceWait(t *testing.T) {
	originalRunPoolTask := runPoolTask
	originalDialerNewConn := dialerNewConn
	defer func() {
		runPoolTask = originalRunPoolTask
		dialerNewConn = originalDialerNewConn
	}()

	old1 := &fakePoolTaskConn{addr: "node-a:8848"}
	old2 := &fakePoolTaskConn{addr: "node-a:8848"}
	dialerNewConn = func(_ context.Context, addr string, _ *dialer.BehaviorOptions) (dialer.Conn, error) {
		t.Fatalf("same-node convergence must not rebuild pool to %s", addr)
		return nil, nil
	}
	runPoolTask = func(d *DBConnectionPool, conn dialer.Conn, task *Task) (model.DataForm, *dialer.ExecutionTrace, error) {
		return nil, &dialer.ExecutionTrace{Failovers: []dialer.FailoverTrace{{
			Reason: dialer.FailoverReasonLeaderConvergenceWait,
			From:   "node-a:8848",
			To:     "node-a:8848",
			Err:    "client error response. <NotLeader>node-a:8848:dnode1",
		}}}, nil
	}

	pool := &DBConnectionPool{
		opt:           PoolOption{Address: "node-a:8848", PoolSize: 2},
		connections:   make(chan pooledConn, 2),
		timeout:       time.Second,
		generation:    1,
		currentLeader: "node-a:8848",
	}
	pool.connections <- pooledConn{conn: old1, generation: 1}
	pool.connections <- pooledConn{conn: old2, generation: 1}

	err := pool.ExecuteTask(&Task{Script: "insert"})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), pool.generation)
	assert.Equal(t, "node-a:8848", pool.currentLeader)
	assert.False(t, old1.closed)
	assert.False(t, old2.closed)
	assert.Equal(t, 2, len(pool.connections))
}

func TestExecuteTaskDoesNotRebuildPoolForLegacySameLeaderTrace(t *testing.T) {
	originalRunPoolTask := runPoolTask
	originalDialerNewConn := dialerNewConn
	defer func() {
		runPoolTask = originalRunPoolTask
		dialerNewConn = originalDialerNewConn
	}()

	old1 := &fakePoolTaskConn{addr: "node-a:8848"}
	old2 := &fakePoolTaskConn{addr: "node-a:8848"}
	dialerNewConn = func(_ context.Context, addr string, _ *dialer.BehaviorOptions) (dialer.Conn, error) {
		t.Fatalf("trace targeting current leader must not rebuild pool to %s", addr)
		return nil, nil
	}
	runPoolTask = func(d *DBConnectionPool, conn dialer.Conn, task *Task) (model.DataForm, *dialer.ExecutionTrace, error) {
		return nil, &dialer.ExecutionTrace{Failovers: []dialer.FailoverTrace{{
			Reason: dialer.FailoverReasonNotLeader,
			From:   "node-a:8848",
			To:     "node-a:8848",
		}}}, nil
	}

	pool := &DBConnectionPool{
		opt:           PoolOption{Address: "node-a:8848", PoolSize: 2},
		connections:   make(chan pooledConn, 2),
		timeout:       time.Second,
		generation:    1,
		currentLeader: "node-a:8848",
	}
	pool.connections <- pooledConn{conn: old1, generation: 1}
	pool.connections <- pooledConn{conn: old2, generation: 1}

	err := pool.ExecuteTask(&Task{Script: "insert"})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), pool.generation)
	assert.False(t, old1.closed)
	assert.False(t, old2.closed)
	assert.Equal(t, 2, len(pool.connections))
}

func TestExecuteTaskSwitchesConnectionsAfterSuccessfulFailoverWithNotLeader(t *testing.T) {
	originalRunPoolTask := runPoolTask
	originalDialerNewConn := dialerNewConn
	defer func() {
		runPoolTask = originalRunPoolTask
		dialerNewConn = originalDialerNewConn
	}()

	old1 := &fakePoolTaskConn{addr: "old-1"}
	old2 := &fakePoolTaskConn{addr: "old-2"}
	created := make([]*fakePoolTaskConn, 0, 1)
	dialerNewConn = func(_ context.Context, addr string, _ *dialer.BehaviorOptions) (dialer.Conn, error) {
		conn := &fakePoolTaskConn{addr: addr}
		created = append(created, conn)
		return conn, nil
	}

	runPoolTask = func(d *DBConnectionPool, conn dialer.Conn, task *Task) (model.DataForm, *dialer.ExecutionTrace, error) {
		return nil, &dialer.ExecutionTrace{
			Failovers: []dialer.FailoverTrace{
				{
					Reason: dialer.FailoverReasonNotLeader,
					From:   "old:8848",
					To:     "leader-target:8848",
				},
				{
					Reason: dialer.FailoverReasonHighAvailability,
					From:   "leader-target:8848",
					To:     "ha-target:8848",
				},
			},
		}, nil
	}

	pool := &DBConnectionPool{
		opt:         PoolOption{Address: "old:8848", UserID: "user", Password: "pwd", PoolSize: 2},
		connections: make(chan pooledConn, 2),
		timeout:     time.Second,
		generation:  1,
	}
	pool.connections <- pooledConn{conn: old1, generation: 1}
	pool.connections <- pooledConn{conn: old2, generation: 1}

	task := &Task{Script: "insert"}
	err := pool.ExecuteTask(task)
	require.NoError(t, err)
	assert.NoError(t, task.GetError())

	require.Len(t, created, 1)
	assert.Equal(t, "ha-target:8848", created[0].addr)
	assert.Equal(t, uint64(2), pool.generation)
	assert.Equal(t, "ha-target:8848", pool.currentLeader)
	assert.True(t, old2.closed)
	assert.Equal(t, 2, len(pool.connections))
}

func TestExecuteTaskKeepsSuccessWhenNotLeaderSwitchFails(t *testing.T) {
	originalRunPoolTask := runPoolTask
	originalDialerNewConn := dialerNewConn
	defer func() {
		runPoolTask = originalRunPoolTask
		dialerNewConn = originalDialerNewConn
	}()

	old1 := &fakePoolTaskConn{addr: "old-1"}
	old2 := &fakePoolTaskConn{addr: "old-2"}
	dialerNewConn = func(_ context.Context, addr string, _ *dialer.BehaviorOptions) (dialer.Conn, error) {
		return nil, errors.New("leader unavailable")
	}

	runPoolTask = func(d *DBConnectionPool, conn dialer.Conn, task *Task) (model.DataForm, *dialer.ExecutionTrace, error) {
		return nil, &dialer.ExecutionTrace{
			Failovers: []dialer.FailoverTrace{{
				Reason: dialer.FailoverReasonNotLeader,
				From:   "old:8848",
				To:     "leader:8848",
			}},
		}, nil
	}

	pool := &DBConnectionPool{
		opt:         PoolOption{Address: "old:8848", PoolSize: 2},
		connections: make(chan pooledConn, 2),
		timeout:     time.Second,
		generation:  1,
	}
	pool.connections <- pooledConn{conn: old1, generation: 1}
	pool.connections <- pooledConn{conn: old2, generation: 1}

	task := &Task{Script: "insert"}
	err := pool.ExecuteTask(task)
	require.NoError(t, err)
	assert.NoError(t, task.GetError())
	// Switch failed, so generation stays at 1 and the recycled conn is returned to the old pool.
	assert.Equal(t, uint64(1), pool.generation)
	assert.Equal(t, 2, len(pool.connections))
}

func TestExecuteTaskRetriesOnNewLeaderWhenNotLeaderExecutionFails(t *testing.T) {
	originalRunPoolTask := runPoolTask
	originalDialerNewConn := dialerNewConn
	defer func() {
		runPoolTask = originalRunPoolTask
		dialerNewConn = originalDialerNewConn
	}()

	oldConn := &fakePoolTaskConn{addr: "old"}
	newConn := &fakePoolTaskConn{addr: "leader:8848"}
	dialerNewConn = func(_ context.Context, addr string, _ *dialer.BehaviorOptions) (dialer.Conn, error) {
		assert.Equal(t, "leader:8848", addr)
		return newConn, nil
	}

	notLeaderErr := errors.New("client error response. <NotLeader>leader:8848:dnode1")
	callCount := 0
	runPoolTask = func(d *DBConnectionPool, conn dialer.Conn, task *Task) (model.DataForm, *dialer.ExecutionTrace, error) {
		callCount++
		if callCount == 1 {
			assert.Same(t, oldConn, conn)
			return nil, &dialer.ExecutionTrace{
				Failovers: []dialer.FailoverTrace{{
					Reason: dialer.FailoverReasonNotLeader,
					From:   "old:8848",
					To:     "leader:8848",
				}},
			}, notLeaderErr
		}

		if callCount == 2 {
			// The retry reuses the borrowed connection before the pool is rebuilt.
			return nil, nil, nil
		}

		t.Fatal("unexpected call")
		return nil, nil, nil
	}

	pool := &DBConnectionPool{
		opt:         PoolOption{Address: "old:8848", PoolSize: 1},
		connections: make(chan pooledConn, 1),
		timeout:     time.Second,
		generation:  1,
	}
	pool.connections <- pooledConn{conn: oldConn, generation: 1}

	task := &Task{Script: "insert"}
	err := pool.ExecuteTask(task)
	require.NoError(t, err)
	assert.NoError(t, task.GetError())
	assert.Equal(t, 2, callCount)
	// With poolSize=1, switchPoolConnectionsToLeader creates 0 new connections
	// and recycles the borrowed conn. oldConn is recycled (not closed).
	assert.False(t, oldConn.closed)
	assert.False(t, newConn.closed)
	assert.Equal(t, 1, len(pool.connections))
}

func TestExecuteTaskUpdatesPoolWhenRetrySucceedsViaAnotherNotLeader(t *testing.T) {
	originalRunPoolTask := runPoolTask
	originalDialerNewConn := dialerNewConn
	defer func() {
		runPoolTask = originalRunPoolTask
		dialerNewConn = originalDialerNewConn
	}()

	old1 := &fakePoolTaskConn{addr: "old-1"}
	old2 := &fakePoolTaskConn{addr: "old-2"}
	created := make([]*fakePoolTaskConn, 0, 1)
	dialerNewConn = func(_ context.Context, addr string, _ *dialer.BehaviorOptions) (dialer.Conn, error) {
		conn := &fakePoolTaskConn{addr: addr}
		created = append(created, conn)
		return conn, nil
	}

	firstErr := errors.New("client error response. <NotLeader>leader-a:8848:dnode1")
	callCount := 0
	runPoolTask = func(d *DBConnectionPool, conn dialer.Conn, task *Task) (model.DataForm, *dialer.ExecutionTrace, error) {
		callCount++
		switch callCount {
		case 1:
			assert.Same(t, old1, conn)
			return nil, &dialer.ExecutionTrace{
				Failovers: []dialer.FailoverTrace{{
					Reason: dialer.FailoverReasonNotLeader,
					From:   "old:8848",
					To:     "leader-a:8848",
				}},
			}, firstErr
		case 2:
			assert.Same(t, old1, conn)
			return nil, &dialer.ExecutionTrace{
				Failovers: []dialer.FailoverTrace{{
					Reason: dialer.FailoverReasonNotLeader,
					From:   "leader-a:8848",
					To:     "leader-b:8848",
				}},
			}, nil
		default:
			t.Fatal("unexpected call")
			return nil, nil, nil
		}
	}

	pool := &DBConnectionPool{
		opt:         PoolOption{Address: "old:8848", PoolSize: 2},
		connections: make(chan pooledConn, 2),
		timeout:     time.Second,
		generation:  1,
	}
	pool.connections <- pooledConn{conn: old1, generation: 1}
	pool.connections <- pooledConn{conn: old2, generation: 1}

	task := &Task{Script: "insert"}
	err := pool.ExecuteTask(task)
	require.NoError(t, err)
	assert.NoError(t, task.GetError())
	assert.Equal(t, 2, callCount)

	require.Len(t, created, 1)
	assert.Equal(t, "leader-b:8848", created[0].addr)
	assert.Equal(t, uint64(2), pool.generation)
	assert.Equal(t, "leader-b:8848", pool.currentLeader)
	assert.True(t, old2.closed)
	assert.False(t, old1.closed)
	assert.False(t, created[0].closed)
	assert.Equal(t, 2, len(pool.connections))
}

func TestExecuteTaskKeepsOriginalErrorWhenNotLeaderSwitchFails(t *testing.T) {
	originalRunPoolTask := runPoolTask
	originalDialerNewConn := dialerNewConn
	defer func() {
		runPoolTask = originalRunPoolTask
		dialerNewConn = originalDialerNewConn
	}()

	old1 := &fakePoolTaskConn{addr: "old-1"}
	old2 := &fakePoolTaskConn{addr: "old-2"}
	dialerNewConn = func(_ context.Context, addr string, _ *dialer.BehaviorOptions) (dialer.Conn, error) {
		return nil, errors.New("leader unavailable")
	}

	taskErr := errors.New("client error response. <NotLeader>leader:8848:dnode1")
	var recycledConn *fakePoolTaskConn
	runPoolTask = func(d *DBConnectionPool, conn dialer.Conn, task *Task) (model.DataForm, *dialer.ExecutionTrace, error) {
		recycledConn = conn.(*fakePoolTaskConn)
		return nil, &dialer.ExecutionTrace{
			Failovers: []dialer.FailoverTrace{{
				Reason: dialer.FailoverReasonNotLeader,
				From:   "old:8848",
				To:     "leader:8848",
			}},
		}, taskErr
	}

	pool := &DBConnectionPool{
		opt:         PoolOption{Address: "old:8848", PoolSize: 2},
		connections: make(chan pooledConn, 2),
		timeout:     time.Second,
		generation:  1,
	}
	pool.connections <- pooledConn{conn: old1, generation: 1}
	pool.connections <- pooledConn{conn: old2, generation: 1}

	task := &Task{Script: "insert"}
	err := pool.ExecuteTask(task)
	require.ErrorIs(t, err, taskErr)
	assert.ErrorIs(t, task.GetError(), taskErr)
	assert.NotContains(t, task.GetError().Error(), "leader unavailable")
	// Switch failed, generation unchanged.
	assert.Equal(t, uint64(1), pool.generation)
	assert.Equal(t, 2, len(pool.connections))
	// Recycled connection was returned to the original pool.
	assert.False(t, recycledConn.closed)
}

func TestReturnConnClosesStaleGenerationConnection(t *testing.T) {
	oldConn := &fakePoolTaskConn{addr: "old"}
	newConn := &fakePoolTaskConn{addr: "new"}
	pool := &DBConnectionPool{
		connections: make(chan pooledConn, 1),
		generation:  2,
	}
	pool.connections <- pooledConn{conn: newConn, generation: 2}

	err := pool.returnConn(pooledConn{conn: oldConn, generation: 1})
	require.NoError(t, err)

	assert.True(t, oldConn.closed)
	assert.Equal(t, 1, len(pool.connections))
	pc := <-pool.connections
	assert.Same(t, newConn, pc.conn)
	assert.False(t, newConn.closed)
}

func TestExecuteTaskRunsFunctionWhenArgsProvided(t *testing.T) {
	original := runPoolTask
	defer func() {
		runPoolTask = original
	}()

	dt, err := model.NewDataType(model.DtInt, int32(42))
	require.NoError(t, err)

	arg := model.NewScalar(dt)
	conn, err := dialer.NewConn(context.TODO(), "127.0.0.1:0", nil)
	require.NoError(t, err)

	var gotScript string
	var gotArgs []model.DataForm
	runPoolTask = func(d *DBConnectionPool, conn dialer.Conn, task *Task) (model.DataForm, *dialer.ExecutionTrace, error) {
		gotScript = task.Script
		gotArgs = task.Args
		return nil, nil, nil
	}

	pool := &DBConnectionPool{
		connections: make(chan pooledConn, 1),
		timeout:     time.Second,
		generation:  1,
	}
	pool.connections <- pooledConn{conn: conn, generation: 1}

	task := &Task{
		Script: "add",
		Args:   []model.DataForm{arg},
	}
	err = pool.ExecuteTask(task)
	require.NoError(t, err)

	assert.Equal(t, "add", gotScript)
	assert.Equal(t, []model.DataForm{arg}, gotArgs)
	assert.True(t, task.IsSuccess())
	assert.Equal(t, 1, len(pool.connections))
}

func TestExecuteTaskReturnsTaskError(t *testing.T) {
	original := runPoolTask
	defer func() {
		runPoolTask = original
	}()

	execErr := errors.New("run failed")
	conn, err := dialer.NewConn(context.TODO(), "127.0.0.1:0", nil)
	require.NoError(t, err)

	runPoolTask = func(d *DBConnectionPool, conn dialer.Conn, task *Task) (model.DataForm, *dialer.ExecutionTrace, error) {
		return nil, nil, execErr
	}

	pool := &DBConnectionPool{
		connections: make(chan pooledConn, 1),
		timeout:     time.Second,
		generation:  1,
	}
	pool.connections <- pooledConn{conn: conn, generation: 1}

	task := &Task{Script: "throw"}
	err = pool.ExecuteTask(task)

	require.ErrorIs(t, err, execErr)
	assert.ErrorIs(t, task.GetError(), execErr)
	assert.False(t, task.IsSuccess())
	assert.Equal(t, 1, len(pool.connections))
}

func TestExecuteTaskRejectsNilTask(t *testing.T) {
	pool := &DBConnectionPool{}

	err := pool.ExecuteTask(nil)

	require.EqualError(t, err, "task must not be nil")
}

func TestExecuteTaskReturnsErrorWhenPoolClosed(t *testing.T) {
	pool := &DBConnectionPool{}
	require.NoError(t, pool.Close())

	task := &Task{Script: "1..10"}
	err := pool.ExecuteTask(task)

	require.EqualError(t, err, "connection pool is closed")
	assert.EqualError(t, task.GetError(), "connection pool is closed")
}

func TestBuildPoolBehaviorOptionsIncludesTimeout(t *testing.T) {
	retries := 3
	opt := &PoolOption{
		Timeout:                  5 * time.Second,
		LeaderConvergenceTimeout: 9 * time.Second,
		EnableHighAvailability:   true,
		HighAvailabilitySites:    []string{"node1:8848"},
		Reconnect:                true,
		TryReconnectNums:         &retries,
		EnableScram:              true,
		SqlStd:                   dialer.SqlStdMySQL,
	}

	behavior := buildPoolBehaviorOptions(opt)
	assert.Equal(t, 5*time.Second, behavior.Timeout)
	assert.Equal(t, 9*time.Second, behavior.LeaderConvergenceTimeout)
	assert.True(t, behavior.EnableHighAvailability)
	assert.Equal(t, []string{"node1:8848"}, behavior.HighAvailabilitySites)
	assert.True(t, behavior.Reconnect)
	assert.Equal(t, &retries, behavior.TryReconnectNums)
	assert.True(t, behavior.EnableScram)
	assert.Equal(t, dialer.SqlStdMySQL, behavior.SqlStd)
}

func TestNewDBConnectionPoolRejectsNegativeLeaderConvergenceTimeout(t *testing.T) {
	_, err := NewDBConnectionPool(&PoolOption{
		Address:                  "127.0.0.1:8848",
		PoolSize:                 1,
		LeaderConvergenceTimeout: -time.Second,
	})
	require.EqualError(t, err, "LeaderConvergenceTimeout must be equal or greater than 0")
}

func TestPoolTraceHelpersIgnoreConvergenceWaitAsNotLeaderTarget(t *testing.T) {
	trace := &dialer.ExecutionTrace{Failovers: []dialer.FailoverTrace{
		{Reason: dialer.FailoverReasonNotLeader, From: "old:8848", To: "leader-a:8848"},
		{Reason: dialer.FailoverReasonLeaderConvergenceWait, From: "leader-a:8848", To: "leader-a:8848"},
	}}

	target, ok := notLeaderTarget(trace)
	require.True(t, ok)
	assert.Equal(t, "leader-a:8848", target)

	target, ok = poolSwitchTarget(trace, nil)
	require.True(t, ok)
	assert.Equal(t, "leader-a:8848", target)

	target, ok = poolSwitchTarget(trace, errors.New("still not leader"))
	require.True(t, ok)
	assert.Equal(t, "leader-a:8848", target)

	sameNodeOnly := &dialer.ExecutionTrace{Failovers: []dialer.FailoverTrace{{
		Reason: dialer.FailoverReasonLeaderConvergenceWait,
		From:   "leader-a:8848",
		To:     "leader-a:8848",
	}}}
	_, ok = notLeaderTarget(sameNodeOnly)
	assert.False(t, ok)
	_, ok = poolSwitchTarget(sameNodeOnly, nil)
	assert.False(t, ok)
}

func TestNewDBConnectionPoolRejectsNonPositiveTryReconnectNums(t *testing.T) {
	zero := 0
	_, err := NewDBConnectionPool(&PoolOption{
		Address:          "127.0.0.1:8848",
		UserID:           "user",
		Password:         "pwd",
		PoolSize:         1,
		Reconnect:        true,
		TryReconnectNums: &zero,
	})
	require.EqualError(t, err, "TryReconnectNums must be nil or greater than 0")

	negative := -1
	_, err = NewDBConnectionPool(&PoolOption{
		Address:          "127.0.0.1:8848",
		UserID:           "user",
		Password:         "pwd",
		PoolSize:         1,
		Reconnect:        true,
		TryReconnectNums: &negative,
	})
	require.EqualError(t, err, "TryReconnectNums must be nil or greater than 0")
}

func TestGetLoadBalanceAddressUsesDiscoveryConnection(t *testing.T) {
	original := openLoadBalanceDiscoveryConn
	defer func() {
		openLoadBalanceDiscoveryConn = original
	}()

	dtl, err := model.NewDataTypeListFromRawData(model.DtString, []string{
		"10.0.0.1:8902:node1",
		"10.0.0.2:8902:node2",
	})
	require.NoError(t, err)

	discoveryConn := &fakeDiscoveryConn{df: model.NewVector(dtl)}
	openLoadBalanceDiscoveryConn = func(opt *PoolOption) (loadBalanceDiscoveryConn, error) {
		assert.True(t, opt.EnableHighAvailability)
		assert.Equal(t, []string{"10.0.0.9:8902", "10.0.0.8:8902"}, opt.HighAvailabilitySites)
		return discoveryConn, nil
	}

	pool := &DBConnectionPool{}
	addresses, err := pool.getLoadBalanceAddress(&PoolOption{
		Address:                "10.0.0.7:8902",
		EnableHighAvailability: true,
		HighAvailabilitySites:  []string{"10.0.0.9:8902", "10.0.0.8:8902"},
	})
	require.NoError(t, err)

	assert.Equal(t, []string{"10.0.0.1:8902", "10.0.0.2:8902"}, addresses)
	assert.True(t, discoveryConn.closed)
}

func TestResolveLoadBalanceAddressesUsesConfiguredAddresses(t *testing.T) {
	original := openLoadBalanceDiscoveryConn
	defer func() {
		openLoadBalanceDiscoveryConn = original
	}()

	openLoadBalanceDiscoveryConn = func(opt *PoolOption) (loadBalanceDiscoveryConn, error) {
		t.Fatal("expected configured load balance addresses to bypass discovery")
		return nil, nil
	}

	pool := &DBConnectionPool{
		loadBalanceAddresses: []string{
			"10.0.0.2:8902",
			"10.0.0.3:8902",
			"10.0.0.2:8902",
		},
	}
	addresses, err := pool.resolveLoadBalanceAddresses(&PoolOption{
		Address:               "10.0.0.3:8902",
		HighAvailabilitySites: []string{"10.0.0.4:8902", "10.0.0.2:8902"},
	})
	require.NoError(t, err)

	assert.Equal(t, []string{
		"10.0.0.3:8902",
		"10.0.0.2:8902",
		"10.0.0.4:8902",
	}, addresses)
}

func TestResolveLoadBalanceAddressesUsesHighAvailabilitySitesWithoutDiscovery(t *testing.T) {
	original := openLoadBalanceDiscoveryConn
	defer func() {
		openLoadBalanceDiscoveryConn = original
	}()

	openLoadBalanceDiscoveryConn = func(opt *PoolOption) (loadBalanceDiscoveryConn, error) {
		t.Fatal("expected configured high availability sites to bypass discovery")
		return nil, nil
	}

	pool := &DBConnectionPool{}
	addresses, err := pool.resolveLoadBalanceAddresses(&PoolOption{
		Address:               "10.0.0.7:8902",
		HighAvailabilitySites: []string{"10.0.0.3:8902", "10.0.0.2:8902"},
	})
	require.NoError(t, err)

	assert.Equal(t, []string{"10.0.0.7:8902", "10.0.0.3:8902", "10.0.0.2:8902"}, addresses)
}

func TestResolveLoadBalanceAddressesFallsBackToDiscoveryWhenNoConfiguredCandidates(t *testing.T) {
	original := openLoadBalanceDiscoveryConn
	defer func() {
		openLoadBalanceDiscoveryConn = original
	}()

	dtl, err := model.NewDataTypeListFromRawData(model.DtString, []string{
		"10.0.0.1:8902:node1",
		"10.0.0.2:8902:node2",
	})
	require.NoError(t, err)

	discoveryConn := &fakeDiscoveryConn{df: model.NewVector(dtl)}
	openLoadBalanceDiscoveryConn = func(opt *PoolOption) (loadBalanceDiscoveryConn, error) {
		return discoveryConn, nil
	}

	pool := &DBConnectionPool{}
	addresses, err := pool.resolveLoadBalanceAddresses(&PoolOption{
		Address: "10.0.0.7:8902",
	})
	require.NoError(t, err)

	assert.Equal(t, []string{"10.0.0.1:8902", "10.0.0.2:8902"}, addresses)
	assert.True(t, discoveryConn.closed)
}

func TestGetLoadBalanceAddressReturnsErrorWhenClusterHasNoAvailableDataNodes(t *testing.T) {
	original := openLoadBalanceDiscoveryConn
	defer func() {
		openLoadBalanceDiscoveryConn = original
	}()

	dtl, err := model.NewDataTypeListFromRawData(model.DtString, []string{})
	require.NoError(t, err)

	discoveryConn := &fakeDiscoveryConn{df: model.NewVector(dtl)}
	openLoadBalanceDiscoveryConn = func(opt *PoolOption) (loadBalanceDiscoveryConn, error) {
		return discoveryConn, nil
	}

	pool := &DBConnectionPool{}
	addresses, err := pool.getLoadBalanceAddress(&PoolOption{
		Address: "10.0.0.7:8902",
	})

	require.Error(t, err)
	assert.Nil(t, addresses)
	assert.Equal(t, "no available data nodes found in the cluster", err.Error())
	assert.True(t, discoveryConn.closed)
}

func TestNewDBConnectionPoolRejectsLoadBalanceAddressesWhenLoadBalanceDisabled(t *testing.T) {
	pool, err := NewDBConnectionPool(&PoolOption{
		Address:              "10.0.0.7:8902",
		PoolSize:             1,
		LoadBalance:          false,
		LoadBalanceAddresses: []string{"10.0.0.8:8902"},
	})

	require.Error(t, err)
	assert.Nil(t, pool)
	assert.Equal(t, "LoadBalanceAddresses requires LoadBalance to be true", err.Error())
}

func TestNewDBConnectionPoolRejectsHighAvailabilitySitesWithoutLoadBalanceOrHighAvailability(t *testing.T) {
	pool, err := NewDBConnectionPool(&PoolOption{
		Address:                "10.0.0.7:8902",
		PoolSize:               1,
		LoadBalance:            false,
		EnableHighAvailability: false,
		HighAvailabilitySites:  []string{"10.0.0.8:8902"},
	})

	require.Error(t, err)
	assert.Nil(t, pool)
	assert.Equal(t, "HighAvailabilitySites requires LoadBalance or EnableHighAvailability to be true", err.Error())
}

func TestLoadBalanceConnOptionUsesResolvedAddressesForHighAvailability(t *testing.T) {
	pool := &DBConnectionPool{}
	opt := &PoolOption{
		EnableHighAvailability: true,
		HighAvailabilitySites:  []string{"10.0.0.9:8902"},
		Timeout:                time.Minute,
		NetTimeout:             5 * time.Second,
	}

	connOpt := pool.loadBalanceConnOption(opt, []string{
		"10.0.0.1:8902",
		"10.0.0.2:8902",
		"10.0.0.3:8902",
	})

	assert.Equal(t, []string{
		"10.0.0.1:8902",
		"10.0.0.2:8902",
		"10.0.0.3:8902",
	}, connOpt.HighAvailabilitySites)
	assert.Equal(t, time.Minute, connOpt.Timeout)
	assert.Equal(t, 5*time.Second, connOpt.NetTimeout)
	assert.Equal(t, []string{"10.0.0.9:8902"}, opt.HighAvailabilitySites)
}
