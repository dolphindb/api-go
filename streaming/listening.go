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

func startListening(c AbstractClient) error {
	if int(c.getSubscriber().listeningPort) == 0 {
		go listening(c, nil)
		return nil
	}

	address := &net.TCPAddr{
		Port: int(c.getSubscriber().listeningPort),
	}

	ln, err := net.ListenTCP(TCPNetWork, address)
	if err != nil {
		return fmt.Errorf("failed to listen on 0.0.0.0:%d: %w", int(c.getSubscriber().listeningPort), err)
	}

	go listening(c, ln)
	return nil
}

func listening(c AbstractClient, ln *net.TCPListener) {
	if ln != nil {
		defer ln.Close()
	}

	ctx, f := context.WithCancel(context.TODO())

	d := &reconnectDetector{
		AbstractClient: c,
	}

	go d.run()

	cs := make([]net.Conn, 0)
	for !c.IsClosed() {
		var conn net.Conn
		var err error
		var ok bool
		var isReversed bool
		if int(c.getSubscriber().listeningPort) == 0 {
			isReversed = true
			conn, ok = c.getConn()
			if !ok {
				runtime.Gosched()
				continue
			}
		} else {
			isReversed = false
			connTcp, err := ln.AcceptTCP()
			if err != nil {
				streamingLogErrorf("failed to accept tcp: %v", err)
				continue
			}
			err = connTcp.SetKeepAlive(true)
			if err != nil {
				streamingLogErrorf("failed to set conn keepAlive: %v", err)
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
