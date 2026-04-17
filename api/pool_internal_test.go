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
	runPoolTask = func(d *DBConnectionPool, conn dialer.Conn, task *Task) (model.DataForm, error) {
		gotTimeout = d.timeout
		gotScript = task.Script
		return result, nil
	}

	pool := &DBConnectionPool{
		connections: make(chan dialer.Conn, 1),
		timeout:     2 * time.Second,
	}
	pool.connections <- conn

	task := &Task{Script: "1..10"}
	err = pool.ExecuteTask(task)
	require.NoError(t, err)

	assert.Equal(t, "1..10", gotScript)
	assert.Equal(t, 2*time.Second, gotTimeout)
	assert.Same(t, result, task.GetResult())
	assert.True(t, task.IsSuccess())
	assert.Equal(t, 1, len(pool.connections))
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
	runPoolTask = func(d *DBConnectionPool, conn dialer.Conn, task *Task) (model.DataForm, error) {
		gotScript = task.Script
		gotArgs = task.Args
		return nil, nil
	}

	pool := &DBConnectionPool{
		connections: make(chan dialer.Conn, 1),
		timeout:     time.Second,
	}
	pool.connections <- conn

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

	runPoolTask = func(d *DBConnectionPool, conn dialer.Conn, task *Task) (model.DataForm, error) {
		return nil, execErr
	}

	pool := &DBConnectionPool{
		connections: make(chan dialer.Conn, 1),
		timeout:     time.Second,
	}
	pool.connections <- conn

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
