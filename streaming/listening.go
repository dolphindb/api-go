package streaming

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"time"
)

func listening(c AbstractClient, port int) {
	address := &net.TCPAddr{
		Port: port,
	}

	ln, err := net.ListenTCP("tcp", address)
	if err != nil {
		panic(fmt.Errorf("failed to listening 0.0.0.0:%d, %w", port, err))
	}

	defer func() {
		err = ln.Close()
		if err != nil {
			fmt.Printf("Failed to close listening tcp server: %s\n", err.Error())
		}
	}()

	ctx, f := context.WithCancel(context.TODO())

	d := &reconnectDetector{
		AbstractClient: c,
	}

	go d.run()

	cs := make([]net.Conn, 0)
	for !c.IsClosed() {
		conn, err := handleData(ctx, ln, c)
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

func handleData(ctx context.Context, ln *net.TCPListener, c AbstractClient) (net.Conn, error) {
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

	if runtime.GOOS != "linux" {
		c := &connectionDetector{
			Conn: conn,
			ctx:  ctx,
		}

		go c.run()
	}

	return conn, nil
}
