package dialer

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/dolphindb/api-go/v3/model"
)

var sleepBeforeRetry = time.Sleep

type node struct {
	address  string
	weight   float64
	fallback string
}

type nodePool struct {
	nodes    []*node
	lastInd  int
	len      int
	lastAddr string
}

// connectToAddress is a test seam for stubbing conn.connect in unit tests.
var connectToAddress = func(c *conn, addr string) error {
	return c.connect(addr)
}

func newNode(address string, weight float64) *node {
	return &node{
		address: address,
		weight:  weight,
	}
}

func newNodeWithFallback(address string, fallback string) *node {
	n := newNode(address, 0)
	n.fallback = fallback
	return n
}

func (n *nodePool) add(no *node) {
	if n.nodes == nil {
		n.nodes = make([]*node, 0)
	}

	for k, c := range n.nodes {
		if c.address == no.address {
			n.nodes[k] = no
			return
		}
	}

	n.nodes = append(n.nodes, no)
	n.len++
}

func isNotInitialized(msg string) bool {
	errMsgs := []string{
		"<ChunkInTransaction>",
		"<DataNodeNotReady>",
		"<ControllerNotReady>",
		"DFS is not enabled",
		"The datanode isn't initialized yet. Please try again later",
	}
	for _, m := range errMsgs {
		if strings.Contains(msg, m) {
			return true
		}
	}
	return false
}

func (n *nodePool) parseError(err error) (ErrorType, string) {
	if err == nil {
		return UNKNOWN, ""
	}

	serverErr, ok := AsServerError(err)
	if ok {
		switch {
		case isNotInitialized(serverErr.Detail):
			return NO_INITIALIZED, ""
		case serverErr.Is(ServerErrNotLeader):
			if serverErr.Address == "" {
				return UNEXPECT, ""
			}
			return NEW_LEADER, serverErr.Address
		case serverErr.Is(ServerErrUnknownLeader):
			return UNKNOWN_LEADER, ""
		case serverErr.Is(ServerErrDataNodeNotAvail):
			return NODE_NOT_AVAIL, ""
		}
	}

	msg := err.Error()
	switch {
	case isNotInitialized(msg):
		return NO_INITIALIZED, ""
	case strings.Contains(msg, "<NotLeader>"):
		return n.getNewLeader(msg)
	case strings.Contains(msg, "<UnknownLeader>"):
		return UNKNOWN_LEADER, ""
	case strings.Contains(msg, "<DataNodeNotAvail>"):
		return NODE_NOT_AVAIL, ""
	case strings.Contains(msg, "Login is required for script execution with client authentication enabled"):
		return LOGIN_REQUIRED, ""
	default:
		return UNKNOWN, ""
	}
}

func (n *nodePool) getNewLeader(msg string) (ErrorType, string) {
	addr := extractTaggedAddr(msg, "<NotLeader>")
	if addr == "" {
		return UNEXPECT, ""
	}

	return NEW_LEADER, addr
}

func (c *conn) getRetryLimit() *int {
	if c.behaviorOpt == nil || (!c.behaviorOpt.Reconnect && !c.behaviorOpt.EnableHighAvailability) {
		return cloneIntPtr(new(int))
	}

	return cloneIntPtr(c.behaviorOpt.TryReconnectNums)
}

func (c *conn) switchDataNode(n *node) (string, error) {
	retryLimit := c.getRetryLimit()
	if retryLimit == nil {
		return c.switchDataNodeWithoutLimit(n)
	}

	return c.switchDataNodeWithAttempts(n, *retryLimit+1)
}

func (c *conn) switchDataNodeWithAttempts(n *node, attempts int) (string, error) {
	if attempts <= 0 {
		return "", fmt.Errorf("failed to connect to %s", c.addr)
	}

	connected := false
	connectedAddress := ""
	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		if n == nil {
			c.dialerLogInfof("starting failover attempt %d/%d from %s", attempt+1, attempts, c.addr)
		}
		if n != nil {
			if connected, connectedAddress, err = c.connectDirectedNode(n); connected {
				return connectedAddress, nil
			}
			n = nil
		} else {
			if connected, connectedAddress, err = c.rangeConnectNode(); connected {
				return connectedAddress, nil
			}
		}
		if err != nil {
			return "", err
		}
		if attempt == attempts-1 {
			c.dialerLogWarnf("failover attempt %d/%d did not connect; no retries left", attempt+1, attempts)
			break
		}

		c.dialerLogWarnf("failover attempt %d/%d did not connect; retrying in 1s", attempt+1, attempts)
		sleepBeforeRetry(time.Second)
	}

	if err != nil {
		return "", err
	}

	return "", fmt.Errorf("failed to connect to %s", c.addr)
}

func (c *conn) switchDataNodeWithoutLimit(n *node) (string, error) {
	connected := false
	connectedAddress := ""
	var err error
	for attempt := 0; ; attempt++ {
		if n == nil {
			c.dialerLogInfof("starting failover attempt %d from %s", attempt+1, c.addr)
		}
		if n != nil {
			if connected, connectedAddress, err = c.connectDirectedNode(n); connected {
				return connectedAddress, nil
			}
			n = nil
		} else {
			if connected, connectedAddress, err = c.rangeConnectNode(); connected {
				return connectedAddress, nil
			}
		}
		if err != nil {
			return "", err
		}

		c.dialerLogWarnf("failover attempt %d did not connect; retrying in 1s", attempt+1)
		sleepBeforeRetry(time.Second)
	}
}

func isRetryableConnectError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EINVAL) {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	var syscallErr *os.SyscallError
	if errors.As(err, &syscallErr) {
		return true
	}

	return errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ENETUNREACH)
}

func (c *conn) rangeConnectNode() (bool, string, error) {
	c.nodePool.lastInd = (c.nodePool.lastInd + 1) % c.nodePool.len
	n := c.nodePool.nodes[c.nodePool.lastInd]
	c.dialerLogDebugf("high-availability connect candidate %s", n.address)
	connected, err := c.connectNode(n)
	if connected {
		return true, n.address, nil
	}

	return false, "", err
}

func (c *conn) connectDirectedNode(n *node) (bool, string, error) {
	c.dialerLogDebugf("server-directed connect candidate %s (fallback=%s)", n.address, n.fallback)
	connected, err := c.connectNode(n)
	if connected || err != nil {
		if connected {
			return true, n.address, nil
		}
		return false, "", err
	}

	if n.fallback == "" || n.fallback == n.address {
		c.dialerLogWarnf("server-directed connect to %s did not connect and no fallback is available", n.address)
		return false, "", nil
	}

	c.dialerLogWarnf("server-directed connect to %s did not connect; trying fallback %s", n.address, n.fallback)
	fallback := newNode(n.fallback, 0)
	connected, err = c.connectNode(fallback)
	if connected {
		return true, fallback.address, nil
	}
	if err == nil {
		c.dialerLogWarnf("fallback connect to %s did not connect", fallback.address)
	}

	return false, "", err
}

// return true, nil: success
// return false, nil: not connected, need to retry
// return false, err: failed
func (c *conn) connectNode(n *node) (bool, error) {
	c.dialerLogDebugf(
		"connecting to %s (NetTimeout=%s)",
		n.address,
		c.connectTimeout(),
	)
	err := connectToAddress(c, n.address)
	if err == nil {
		c.isPublicName = c.isPublicNameAddress(n.address)
		c.dialerLogInfof("connection to %s is ready", n.address)
		return true, nil
	}
	if isRetryableConnectError(err) {
		c.dialerLogWarnf("connect to %s failed with retryable error: %v", n.address, err)
		return false, nil
	}

	et, _ := c.nodePool.parseError(err)
	if et == UNEXPECT || et == UNKNOWN || et == LOGIN_REQUIRED {
		c.dialerLogErrorf("connect to %s failed: %v", n.address, err)
		return false, err
	}

	c.dialerLogWarnf("connect to %s failed with handled server state: %v", n.address, err)
	return false, nil
}

func isVariableCandidate(word string) bool {
	if len(word) == 0 {
		return false
	}
	if cur := word[0]; (cur < 'a' || cur > 'z') && (cur < 'A' || cur > 'Z') {
		return false
	}

	for _, cur := range word {
		if (cur < 'a' || cur > 'z') && (cur < 'A' || cur > 'Z') && (cur < '0' || cur > '9') && cur != '_' {
			return false
		}
	}

	return true
}

func (c *conn) connected() bool {
	_, di, err := c.runInternal(&requestParams{
		commandType: scriptCmd,
		Command:     generateScriptCommand("1+1"),
	})

	if err != nil {
		c.dialerLogWarnf("connection health probe failed: %v", err)
		return false
	}

	s, ok := di.(*model.Scalar)
	if !ok {
		c.dialerLogWarnf("connection health probe returned unexpected response type %T", di)
		return false
	}

	healthy := s.Value().(int32) == 2
	if !healthy {
		c.dialerLogWarnf("connection health probe returned unexpected value: %v", s.Value())
	}

	return healthy
}
