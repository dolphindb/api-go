package api

import (
	"github.com/dolphindb/api-go/v3/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
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
