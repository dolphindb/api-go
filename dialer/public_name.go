package dialer

import (
	"fmt"
	"net"
	"strings"

	"github.com/dolphindb/api-go/v3/model"
)

const (
	clusterSiteDiscoveryScript = "select site from getClusterPerf()"
	publicNameConfigName       = "publicName"
)

type addressPair struct {
	local  string
	public string
}

func (p addressPair) preferred(usePublic bool) (first string, second string) {
	if usePublic && p.public != "" {
		return p.public, p.local
	}

	return p.local, p.public
}

func (c *conn) refreshPublicNameAddressMap(connectedAddress string) {
	if !c.enableHighAvailability && !c.reconnect {
		return
	}

	mapping, err := c.discoverPublicNameAddressMap()
	if err != nil {
		c.dialerLogDebugf("failed to discover publicName address map: %v", err)
		return
	}
	if len(mapping) == 0 {
		return
	}

	c.publicNameByAddress = mapping
	c.addressByPublicName = reverseAddressMap(mapping)
	c.isPublicName = c.isPublicNameAddress(connectedAddress)
}

func (c *conn) discoverPublicNameAddressMap() (map[string]string, error) {
	_, df, err := runInternalRequest(c, &requestParams{
		commandType: scriptCmd,
		Command:     generateScriptCommand(clusterSiteDiscoveryScript),
	})
	if err != nil {
		return nil, err
	}

	table, ok := df.(*model.Table)
	if !ok {
		return nil, fmt.Errorf("expected getClusterPerf site table, got %T", df)
	}

	siteCol := table.GetColumnByName("site")
	if siteCol == nil || siteCol.Data == nil {
		return nil, fmt.Errorf("getClusterPerf result does not contain site column")
	}

	sites := siteCol.Data.StringList()
	mapping := make(map[string]string, len(sites))
	for _, site := range sites {
		internalAddress, alias, err := parseClusterSite(site)
		if err != nil {
			c.dialerLogDebugf("skip invalid cluster site %q: %v", site, err)
			continue
		}

		publicName, err := c.getNodePublicName(alias)
		if err != nil {
			c.dialerLogDebugf("failed to get publicName for %s: %v", alias, err)
			continue
		}

		publicAddress := normalizePublicNameAddress(publicName, internalAddress)
		if publicAddress == "" {
			continue
		}
		mapping[internalAddress] = publicAddress
	}

	return mapping, nil
}

func (c *conn) getNodePublicName(alias string) (string, error) {
	_, df, err := runInternalRequest(c, &requestParams{
		commandType: scriptCmd,
		Command:     generateScriptCommand(fmt.Sprintf("rpc(`%s, getConfig, `%s)", alias, publicNameConfigName)),
	})
	if err != nil {
		return "", err
	}

	switch v := df.(type) {
	case *model.Scalar:
		if v.IsNull() {
			return "", nil
		}
		value, ok := v.Value().(string)
		if !ok {
			return "", fmt.Errorf("expected scalar string publicName for %s, got %T", alias, v.Value())
		}
		return value, nil
	case *model.Vector:
		if v.Rows() == 0 || v.Data == nil {
			return "", nil
		}
		values := v.Data.StringList()
		if len(values) == 0 {
			return "", nil
		}
		return values[0], nil
	default:
		return "", fmt.Errorf("expected publicName scalar or vector for %s, got %T", alias, df)
	}
}

func parseClusterSite(raw string) (address string, alias string, err error) {
	raw = strings.TrimSpace(raw)
	lastColon := strings.LastIndex(raw, ":")
	if lastColon < 0 || lastColon == len(raw)-1 {
		return "", "", fmt.Errorf("invalid cluster site: %s", raw)
	}

	host, port, err := net.SplitHostPort(raw[:lastColon])
	if err != nil {
		return "", "", fmt.Errorf("invalid cluster site: %s", raw)
	}

	return net.JoinHostPort(host, port), raw[lastColon+1:], nil
}

func normalizePublicNameAddress(publicName, fallbackAddress string) string {
	publicName = strings.TrimSpace(publicName)
	if publicName == "" {
		return ""
	}

	if host, port, err := net.SplitHostPort(publicName); err == nil {
		return net.JoinHostPort(host, port)
	}

	_, port, err := net.SplitHostPort(fallbackAddress)
	if err != nil {
		return parseAddr(publicName)
	}

	return net.JoinHostPort(publicName, port)
}

func reverseAddressMap(mapping map[string]string) map[string]string {
	reversed := make(map[string]string, len(mapping))
	for address, publicName := range mapping {
		if address == "" || publicName == "" {
			continue
		}
		reversed[publicName] = address
	}

	return reversed
}

func (c *conn) serverDirectedAddressPair(address string) addressPair {
	if address == "" {
		return addressPair{}
	}

	pair := addressPair{
		local:  address,
		public: c.publicNameByAddress[address],
	}
	if pair.public == "" {
		if local, ok := c.addressByPublicName[address]; ok {
			pair.local = local
			pair.public = address
		}
	}

	return pair
}

func (c *conn) isPublicNameAddress(address string) bool {
	_, ok := c.addressByPublicName[address]
	return ok
}
