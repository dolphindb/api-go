package dialer

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/dolphindb/api-go/v3/model"
)

type node struct {
	address string
	weight  float64
}

type nodePool struct {
	nodes    []*node
	lastInd  int
	len      int
	lastAddr string
}

func newNode(address string, weight float64) *node {
	return &node{
		address: address,
		weight:  weight,
	}
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

func (n *nodePool) parseError(msg string, no *node) ErrorType {
	switch {
	case isNotInitialized(msg):
		return NO_INITIALIZED
	case strings.Contains(msg, "<NotLeader>"):
		return n.getNewLeader(msg, no)
	case strings.Contains(msg, "<DataNodeNotAvail>"):
		return n.handleNotAvailError(msg, no)
	case strings.Contains(msg, "Login is required for script execution with client authentication enabled"):
		return LOGIN_REQUIRED
	default:
		return UNKNOWN
	}
}

func (n *nodePool) handleNotAvailError(msg string, no *node) ErrorType {
	ind := strings.Index(msg, ">")
	raw := msg[:ind+1]
	addr := parseAddr(raw)
	if addr == "" {
		return UNEXPECT
	}

	no.address = addr
	return NODE_NOT_AVAIL
}

func (n *nodePool) getNewLeader(msg string, no *node) ErrorType {
	ind := strings.Index(msg, ">")
	raw := msg[:ind+1]
	addr := parseAddr(raw)
	if addr == "" {
		return UNEXPECT
	}

	no.address = addr
	fmt.Println("New leader is ", addr)
	return NEW_LEADER
}

func (c *conn) getRetryTimes() int {
	if c.behaviorOpt == nil || (!c.behaviorOpt.Reconnect && !c.behaviorOpt.EnableHighAvailability) {
		return 0
	}

	if c.behaviorOpt.TryReconnectNums == nil {
		return math.MaxInt32 // HACK: mock for try forever
	}

	return *c.behaviorOpt.TryReconnectNums
}

func (c *conn) switchDataNode(n *node) (err error) {
	retryTimes := c.getRetryTimes()
	connected := false
	for attempt := 0; attempt <= retryTimes; attempt++ { // at least try once
		if n != nil {
			if connected, err = c.connectNode(n); connected {
				return nil
			}
			n = nil
		} else {
			if connected, err = c.rangeConnectNode(); connected {
				return nil
			}
		}
		if err != nil {
			return err
		}

		time.Sleep(time.Second)
	}

	if err != nil {
		return err
	}

	return fmt.Errorf("failed to connect to %s", c.addr)
}

func isRetryableConnectError(err error) bool {
	if err == nil {
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

func (c *conn) rangeConnectNode() (bool, error) {
	c.nodePool.lastInd = (c.nodePool.lastInd + 1) % c.nodePool.len
	return c.connectNode(c.nodePool.nodes[c.nodePool.lastInd])
}

// return true, nil: success
// return false, nil: not connected, need to retry
// return false, err: failed
func (c *conn) connectNode(n *node) (bool, error) {
	fmt.Println("Connect to ", n.address)
	err := c.connect(n.address)
	if err == nil {
		return true, nil
	}
	if isRetryableConnectError(err) {
		fmt.Printf("Connect to %s failed: %s\n", n.address, err)
		return false, nil
	}

	node := newNode("", 0)
	et := c.nodePool.parseError(err.Error(), node)
	if et == UNEXPECT || et == UNKNOWN || et == LOGIN_REQUIRED {
		fmt.Printf("Connect to %s failed: %s\n", n.address, err)
		return false, err
	}

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
		return false
	}

	s, ok := di.(*model.Scalar)
	if !ok {
		return false
	}

	return s.Value().(int32) == 2
}

// NOTE function for get lowest load node

// func (c *conn) getConnectedNode() (*node, error) {
// 	for !c.isConnected {
// 		for _, v := range c.nodePool.nodes {
// 			ok, err := c.connectNode(v)
// 			if err != nil {
// 				return nil, err
// 			}
// 			if ok {
// 				return v, nil
// 			}
// 			time.Sleep(100 * time.Millisecond)
// 		}
// 	}

// 	return nil, nil
// }

// func (c *conn) connectMinNode() error {
// 	connectedNode, table, err := c.getClusterPerf()
// 	if err != nil {
// 		return err
// 	}

// 	if c.loadBalance {
// 		err = c.connectLoadBalance(table, connectedNode)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

// func (c *conn) connectLoadBalance(tb *model.Table, cn *node) error {
// 	c.calculateNodeWeight(tb)
// 	minNode := c.nodePool.nodes[0]
// 	for _, v := range c.nodePool.nodes {
// 		if v.weight < minNode.weight {
// 			minNode = v
// 		}
// 	}

// 	if minNode.address != cn.address {
// 		fmt.Println("Connect to min load node: ", minNode.address)
// 		c.Conn.Close()
// 		err := c.switchDataNode(minNode)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

// func (c *conn) calculateNodeWeight(tb *model.Table) {
// 	colHost := tb.GetColumnByName("host")
// 	colPort := tb.GetColumnByName("port")
// 	colMode := tb.GetColumnByName("mode")
// 	colMaxConnections := tb.GetColumnByName("maxConnections")
// 	colConnectionNum := tb.GetColumnByName("connectionNum")
// 	colWorkerNum := tb.GetColumnByName("workerNum")
// 	colExecutorNum := tb.GetColumnByName("executorNum")
// 	load := 0.0
// 	for k, v := range colMode.Data.StringList() {
// 		if v == "0" {
// 			nodeHost := colHost.Data.ElementString(k)
// 			nodePort := colPort.Data.ElementString(k)
// 			var existNode *node
// 			if c.highAvailabilitySites != nil {
// 				for _, n := range c.nodePool.nodes {
// 					if n.address == fmt.Sprintf("%s:%s", nodeHost, nodePort) {
// 						existNode = n
// 						break
// 					}
// 				}

// 				if existNode == nil {
// 					continue
// 				}
// 			}

// 			if colExecutorNum.Data.ElementValue(k).(int32) < colMaxConnections.Data.ElementValue(k).(int32) {
// 				load = float64(colConnectionNum.Data.ElementValue(k).(int32)+
// 					colWorkerNum.Data.ElementValue(k).(int32)+colExecutorNum.Data.ElementValue(k).(int32)) / 3.0
// 			} else {
// 				load = math.MaxFloat64
// 			}

// 			if existNode != nil {
// 				existNode.weight = load
// 			} else {
// 				c.nodePool.add(&node{address: fmt.Sprintf("%s:%s", nodeHost, nodePort), weight: load})
// 			}
// 		}
// 	}
// }

// func (c *conn) getClusterPerf() (*node, *model.Table, error) {
// 	var connectedNode *node
// 	var table *model.Table
// 	var err error
// 	n := newNode("", 1)
// 	for !c.isClosed {
// 		connectedNode, err = c.getConnectedNode()
// 		if err != nil {
// 			return nil, nil, err
// 		}

// 		df, err := c.RunScript("rpc(getControllerAlias(), getClusterPerf)")
// 		if err != nil {
// 			err = c.handleGetClusterPerfError(n, err)
// 			if err != nil {
// 				return nil, nil, err
// 			}

// 			continue
// 		}

// 		table = df.(*model.Table)
// 		break
// 	}

// 	if table == nil {
// 		return nil, nil, errors.New("run getClusterPerf() failed")
// 	}

// 	return connectedNode, table, nil
// }

// func (c *conn) handleGetClusterPerfError(n *node, err error) error {
// 	fmt.Println("ERROR getting other data nodes, error: ", err)
// 	n1 := &node{}
// 	if c.isConnected {
// 		et := c.nodePool.parseError(err.Error(), n)
// 		if et == IGNORE {
// 			return nil
// 		} else if et == NEW_LEADER || et == NODE_NOT_AVAIL {
// 			err = c.switchDataNode(n1)
// 			if err != nil {
// 				return err
// 			}
// 		}
// 	} else {
// 		err = c.switchDataNode(n1)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }
