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
			tryReconnect(val, r.AbstractClient)
			return true
		})

		time.Sleep(1 * time.Second)
	}
}

func (r *reconnectDetector) handleReconnectSites(site string) error {
	if getReconnectItemState(site) == 1 {
		return r.reconnectWithSite(site)
	}

	ts := getReconnectTimestamp(site)
	if time.Now().UnixNano()/1000000 >= ts+3000 {
		s := getSiteByName(site)
		err := r.activeCloseConnection(s)
		if err != nil {
			fmt.Printf("Failed to reconnect closed connection: %s\n", err.Error())
			return err
		}

		for _, v := range getAllTopicBySite(site) {
			tryReconnect(v, r.AbstractClient)
		}

		setReconnectTimestamp(site, time.Now().UnixNano()/1000000)
	}

	return nil
}

func (r *reconnectDetector) reconnectWithSite(site string) error {
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
		tryReconnect(topic, r.AbstractClient)
		lastTopic = topic
	}

	setReconnectItem(lastTopic, 2)
	return nil
}
