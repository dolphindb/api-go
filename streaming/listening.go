package streaming

import (
	"context"
	"fmt"
	"net"
	"runtime"
)

var (
	TCPNetWork = "tcp"
)

func listening(c AbstractClient) {
	address := &net.TCPAddr{
		Port: int(c.getSubscriber().listeningPort),
	}

	ln, err := net.ListenTCP(TCPNetWork, address)
	if err != nil {
		panic(fmt.Errorf("failed to listening 0.0.0.0:%d, %w", int(c.getSubscriber().listeningPort), err))
	}

	defer ln.Close()

	ctx, f := context.WithCancel(context.TODO())

	d := &reconnectDetector{
		AbstractClient: c,
	}

	go d.run()

	cs := make([]net.Conn, 0)
	for !c.IsClosed() {
		// fmt.Println("subscriber listening new connection")
		// HACK use print to avoid stuck of regression test
		fmt.Print("")
		var conn net.Conn
		var ok bool
		var isReversed bool
		if int(c.getSubscriber().listeningPort) == 0 {
			isReversed = true
			conn, ok = c.getConn()
			if !ok {
				runtime.Gosched()
				continue;
			}
		} else {
			isReversed = false
			connTcp, err := ln.AcceptTCP()
			if err != nil {
				fmt.Printf("Failed to accept tcp: %s\n", err.Error())
				continue
			}
			err = connTcp.SetKeepAlive(true)
			if err != nil {
				fmt.Printf("Failed to set conn keepAlive: %s\n", err.Error())
				continue
			}
			conn = connTcp
		}

		err = receiveData(ctx, conn, c, isReversed)
		if err != nil {
			runtime.Gosched()
			// time.Sleep(100 * time.Millisecond)
			continue
		}

		cs = append(cs, conn)
	}

	f()
	for _, v := range cs {
		v.Close()
	}
}

func receiveData(ctx context.Context, conn net.Conn, c AbstractClient, isReversed bool) error {
	mp := &messageParser{
		ctx:              ctx,
		Conn:             conn,
		subscriber:       c.getSubscriber(),
		topicNameToIndex: make(map[string]map[string]int),
		isReversed:       isReversed,
	}

	go mp.run()

	return nil
}
