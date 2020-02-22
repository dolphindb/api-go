package main

import (
	"../api"
	"fmt"
	"math/rand"
	"time"
)

const (
	host = "127.0.0.1"
	port = 8848
)

func main() {
	var client ddb.PollingClient
	listenport := rand.Intn(1000) + 50000
	client.New(listenport)

	queue := client.Subscribe(host, port, "st1", ddb.Def_action_name(), 0)
	msg := ddb.CreateConstant(ddb.DT_VOID)
	for true {
		if queue.Poll(msg, 1000) {
			if msg.IsNull() {
				break
			}
			fmt.Println("Get message at", time.Now().String())
		}
	}
	client.UnSubscribe(host, port, "st1", ddb.Def_action_name())
}
