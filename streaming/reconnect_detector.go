package streaming

import (
	"fmt"
	"time"
)

type reconnectDetector struct {
	AbstractClient
}

func (r *reconnectDetector) run() {
	for !r.IsClosed() {
		for _, site := range getAllReconnectSites() {
			err := r.handleReconnectSites(site)
			if err != nil {
				return
			}
		}

		waitReconnectTopic.Range(func(k, v interface{}) bool {
			val := k.(string)
			r.tryReconnect(val)
			return true
		})

		time.Sleep(1 * time.Second)
	}
}

func (r *reconnectDetector) handleReconnectSites(site string) error {
	if getNeedReconnect(site) == 1 {
		s := getSiteByName(site)
		if s == nil {
			return nil
		}

		err := r.activeCloseConnection(s)
		if err != nil {
			fmt.Printf("Failed to reconnect closed connection: %s\n", err.Error())
			return err
		}

		lastTopic := ""
		for _, topic := range getAllTopicBySite(site) {
			r.tryReconnect(topic)
			lastTopic = topic
		}

		setNeedReconnect(lastTopic, 2)
	} else {
		ts := getReconnectTimestamp(site)
		if time.Now().UnixNano()/1000000 >= ts+3000 {
			s := getSiteByName(site)
			err := r.activeCloseConnection(s)
			if err != nil {
				fmt.Printf("Failed to reconnect closed connection: %s\n", err.Error())
				return err
			}

			for _, v := range getAllTopicBySite(site) {
				r.tryReconnect(v)
			}

			setReconnectTimestamp(site, time.Now().UnixNano()/1000000)
		}
	}

	return nil
}
