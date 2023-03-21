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
		var conn *net.TCPConn
		if int(c.getSubscriber().listeningPort) == 0 {
			conn = c.getTCPConn()
		} else {
			conn, err = ln.AcceptTCP()
			if err != nil {
				fmt.Printf("Failed to accept tcp: %s\n", err.Error())
				continue
			}
		}

		err = conn.SetKeepAlive(true)
		if err != nil {
			fmt.Printf("Failed to set conn keepAlive: %s\n", err.Error())
			continue
		}

		err = receiveData(ctx, conn, c)
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

func receiveData(ctx context.Context, conn *net.TCPConn, c AbstractClient) error {
	mp := &messageParser{
		ctx:              ctx,
		Conn:             conn,
		subscriber:       c.getSubscriber(),
		topicNameToIndex: make(map[string]map[string]int),
	}

	go mp.run()

	return nil
}
