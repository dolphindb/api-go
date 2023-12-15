package streaming

import (
	"fmt"
	"runtime"
	"time"
)

type reconnectDetector struct {
	AbstractClient
}

func (r *reconnectDetector) run() {
	for !r.IsClosed() {
		// HACK use print to avoid stuck of regression test
		fmt.Print("")
		// fmt.Println("streaming reconnect detecting")
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

		runtime.Gosched()
		// time.Sleep(1 * time.Second)
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
