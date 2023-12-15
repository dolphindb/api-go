package dialer

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/dolphindb/api-go/model"
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

func (n *nodePool) parseError(msg string, no *node) ErrorType {
	switch {
	case strings.Contains(msg, "<NotLeader>"):
		return n.getNewLeader(msg, no)
	case strings.Contains(msg, "<DataNodeNotAvail>"):
		return n.handleNotAvailError(msg, no)
	case strings.Contains(msg, "The datanode isn't initialized yet. Please try again later"):
		return NOINITIALIZED
	default:
		return UNKNOW
	}
}

func (n *nodePool) handleNotAvailError(msg string, no *node) ErrorType {
	ind := strings.Index(msg, ">")
	raw := msg[:ind+1]
	addr := parseAddr(raw)
	if addr == "" {
		return UNEXPECT
	}

	no.address = ""
	return NODENOTAVAIL
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
	return NEWLEADER
}

func (c *conn) switchDatanode(n *node) (err error) {
	connected := false
	for !connected {
		if n.address != "" {
			ok, err := c.connectNode(n)
			if err != nil {
				return err
			}
			if ok {
				connected = true
				break
			}
		}

		connected, err = c.rangeConnectNode(n)
		if err != nil {
			return err
		}

		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

func (c *conn) rangeConnectNode(n *node) (bool, error) {
	if c.nodePool.len == 0 {
		return false, errors.New("Failed to connect to " + n.address)
	}

	for i := c.nodePool.len - 1; i >= 0; i-- {
		c.nodePool.lastInd = (c.nodePool.lastInd + 1) % c.nodePool.len
		ok, err := c.connectNode(c.nodePool.nodes[c.nodePool.lastInd])
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}

	return false, nil
}

func (c *conn) getConnectedNode() (*node, error) {
	for !c.isConnected {
		for _, v := range c.nodePool.nodes {
			ok, err := c.connectNode(v)
			if err != nil {
				return nil, err
			}
			if ok {
				return v, nil
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	return nil, nil
}

func (c *conn) connectMinNode() (bool, error) {
	connectedNode, table, err := c.getClusterPerf()
	if err != nil {
		return false, err
	}

	if c.loadBalance {
		err = c.connectLoadBalance(table, connectedNode)
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

func (c *conn) connectLoadBalance(tb *model.Table, cn *node) error {
	c.calculateNodeWeight(tb)
	minNode := c.nodePool.nodes[0]
	for _, v := range c.nodePool.nodes {
		if v.weight < minNode.weight {
			minNode = v
		}
	}

	if minNode.address != cn.address {
		fmt.Println("Connect to min load node: ", minNode.address)
		c.Conn.Close()
		err := c.switchDatanode(minNode)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *conn) calculateNodeWeight(tb *model.Table) {
	colHost := tb.GetColumnByName("host")
	colPort := tb.GetColumnByName("port")
	colMode := tb.GetColumnByName("mode")
	colMaxConnections := tb.GetColumnByName("maxConnections")
	colConnectionNum := tb.GetColumnByName("connectionNum")
	colWorkerNum := tb.GetColumnByName("workerNum")
	colExecutorNum := tb.GetColumnByName("executorNum")
	load := 0.0
	for k, v := range colMode.Data.StringList() {
		if v == "0" {
			nodeHost := colHost.Data.ElementString(k)
			nodePort := colPort.Data.ElementString(k)
			var existNode *node
			if c.highAvailabilitySites != nil {
				for _, n := range c.nodePool.nodes {
					if n.address == fmt.Sprintf("%s:%s", nodeHost, nodePort) {
						existNode = n
						break
					}
				}

				if existNode == nil {
					continue
				}
			}

			if colExecutorNum.Data.ElementValue(k).(int32) < colMaxConnections.Data.ElementValue(k).(int32) {
				load = float64(colConnectionNum.Data.ElementValue(k).(int32)+
					colWorkerNum.Data.ElementValue(k).(int32)+colExecutorNum.Data.ElementValue(k).(int32)) / 3.0
			} else {
				load = math.MaxFloat64
			}

			if existNode != nil {
				existNode.weight = load
			} else {
				c.nodePool.add(&node{address: fmt.Sprintf("%s:%s", nodeHost, nodePort), weight: load})
			}
		}
	}
}

func (c *conn) getClusterPerf() (*node, *model.Table, error) {
	var connectedNode *node
	var table *model.Table
	var err error
	n := newNode("", 1)
	for !c.isClosed {
		connectedNode, err = c.getConnectedNode()
		if err != nil {
			return nil, nil, err
		}

		df, err := c.RunScript("rpc(getControllerAlias(), getClusterPerf)")
		if err != nil {
			err = c.handleGetClusterPerfError(n, err)
			if err != nil {
				return nil, nil, err
			}

			continue
		}

		table = df.(*model.Table)
		break
	}

	if table == nil {
		return nil, nil, errors.New("Run getClusterPerf() failed.")
	}

	return connectedNode, table, nil
}

func (c *conn) handleGetClusterPerfError(n *node, err error) error {
	fmt.Println("ERROR getting other data nodes, error: ", err)
	n1 := &node{}
	if c.isConnected {
		et := c.nodePool.parseError(err.Error(), n)
		if et == IGNORE {
			return nil
		} else if et == NEWLEADER || et == NODENOTAVAIL {
			err = c.switchDatanode(n1)
			if err != nil {
				return err
			}
		}
	} else {
		err = c.switchDatanode(n1)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *conn) connectNode(n *node) (bool, error) {
	fmt.Println("Connect to ", n.address)
	for !c.isClosed {
		err := c.connect(n.address)
		if err != nil {
			if c.isConnected {
				node := newNode("", 0)
				et := c.nodePool.parseError(err.Error(), node)
				switch {
				case et == IGNORE:
					return true, nil
				case et == NODENOTAVAIL, et == NOINITIALIZED:
					return false, nil
				case et != NEWLEADER:
					return false, err
				}
			} else {
				fmt.Printf("Connect to %s failed: %s\n", n.address, err)
				return false, nil
			}

			time.Sleep(100 * time.Millisecond)
			continue
		}

		return true, nil
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
