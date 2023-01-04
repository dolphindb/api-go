package streaming

import (
	"context"
	"fmt"
	"net"
	"time"
)

var (
	TCPNetWork = "tcp"
)

func listening(c AbstractClient, port int) {
	address := &net.TCPAddr{
		Port: port,
	}

	ln, err := net.ListenTCP(TCPNetWork, address)
	if err != nil {
		panic(fmt.Errorf("failed to listening 0.0.0.0:%d, %w", port, err))
	}

	defer ln.Close()

	ctx, f := context.WithCancel(context.TODO())

	d := &reconnectDetector{
		AbstractClient: c,
	}

	go d.run()

	cs := make([]net.Conn, 0)
	for !c.IsClosed() {
		conn, err := receiveData(ctx, ln, c)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		cs = append(cs, conn)
	}

	f()
	for _, v := range cs {
		v.Close()
	}
}

func receiveData(ctx context.Context, ln *net.TCPListener, c AbstractClient) (net.Conn, error) {
	conn, err := ln.AcceptTCP()
	if err != nil {
		fmt.Printf("Failed to accept tcp: %s\n", err.Error())
		return nil, err
	}

	err = conn.SetKeepAlive(true)
	if err != nil {
		fmt.Printf("Failed to set conn keepAlive: %s\n", err.Error())
		return nil, err
	}

	mp := &messageParser{
		ctx:              ctx,
		Conn:             conn,
		subscriber:       c.getSubscriber(),
		topicNameToIndex: make(map[string]map[string]int),
	}

	go mp.run()

	return conn, nil
}
