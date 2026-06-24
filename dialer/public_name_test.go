package dialer

import (
	"strings"
	"testing"

	"github.com/dolphindb/api-go/v3/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiscoverPublicNameAddressMap(t *testing.T) {
	originalRunInternal := runInternalRequest
	defer func() {
		runInternalRequest = originalRunInternal
	}()

	siteList, err := model.NewDataTypeListFromRawData(model.DtString, []string{
		"10.0.0.1:8848:dnode1",
		"10.0.0.2:8848:dnode2",
	})
	require.NoError(t, err)
	siteTable, err := model.NewTable([]string{"site"}, []*model.Vector{model.NewVector(siteList)})
	require.NoError(t, err)

	public1, err := model.NewDataType(model.DtString, "public-1.example.com")
	require.NoError(t, err)
	public2, err := model.NewDataType(model.DtString, "public-2.example.com:18848")
	require.NoError(t, err)

	runInternalRequest = func(c *conn, params *requestParams) (*responseHeader, model.DataForm, error) {
		script := string(params.Command)
		switch {
		case strings.Contains(script, clusterSiteDiscoveryScript):
			return nil, siteTable, nil
		case strings.Contains(script, "rpc(`dnode1, getConfig, `publicName)"):
			return nil, model.NewScalar(public1), nil
		case strings.Contains(script, "rpc(`dnode2, getConfig, `publicName)"):
			return nil, model.NewScalar(public2), nil
		default:
			t.Fatalf("unexpected script: %s", script)
			return nil, nil, nil
		}
	}

	c := &conn{isConnected: true}
	mapping, err := c.discoverPublicNameAddressMap()
	require.NoError(t, err)

	assert.Equal(t, map[string]string{
		"10.0.0.1:8848": "public-1.example.com:8848",
		"10.0.0.2:8848": "public-2.example.com:18848",
	}, mapping)
}

func TestParseClusterSite(t *testing.T) {
	address, alias, err := parseClusterSite("192.168.0.69:8803:dnode2")

	require.NoError(t, err)
	assert.Equal(t, "192.168.0.69:8803", address)
	assert.Equal(t, "dnode2", alias)
}

func TestNormalizePublicNameAddressUsesFallbackPort(t *testing.T) {
	assert.Equal(t, "public.example.com:8848", normalizePublicNameAddress("public.example.com", "10.0.0.1:8848"))
	assert.Equal(t, "public.example.com:18848", normalizePublicNameAddress("public.example.com:18848", "10.0.0.1:8848"))
}

func TestServerDirectedAddressPairPreferredFollowsCurrentConnectionAddressType(t *testing.T) {
	c := &conn{
		publicNameByAddress: map[string]string{
			"10.0.0.2:8848": "public.example.com:8848",
		},
		addressByPublicName: map[string]string{
			"public.example.com:8848": "10.0.0.2:8848",
		},
	}

	pair := c.serverDirectedAddressPair("10.0.0.2:8848")
	first, second := pair.preferred(c.isPublicName)
	assert.Equal(t, "10.0.0.2:8848", first)
	assert.Equal(t, "public.example.com:8848", second)

	c.isPublicName = true
	first, second = pair.preferred(c.isPublicName)
	assert.Equal(t, "public.example.com:8848", first)
	assert.Equal(t, "10.0.0.2:8848", second)
}
