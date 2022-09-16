package streaming

import (
	"context"
	"fmt"
	"net"
	"time"
)

type connectionDetector struct {
	net.Conn
	ctx context.Context
}

func (c *connectionDetector) run() {
	for !c.IsClosed() {
		_, err := c.Write([]byte{0xff})
		if err != nil {
			failCount := 0
			for i := 0; i < 5; i++ {
				_, err = c.Write([]byte{0xff})
				if err != nil {
					failCount++
				}

				time.Sleep(1000 * time.Millisecond)
			}

			if failCount != 5 {
				continue
			}

			c.Close()
			fmt.Println("Connection closed!!")
			return
		}

		time.Sleep(1000 * time.Millisecond)
	}
}

func (c *connectionDetector) IsClosed() bool {
	select {
	case <-c.ctx.Done():
		return true
	default:
		return false
	}
}
