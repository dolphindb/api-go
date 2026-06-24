package main

import (
	"context"

	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/dolphindb/api-go/v3/dolphindb"
	"github.com/dolphindb/api-go/v3/example/apis"
	"github.com/dolphindb/api-go/v3/example/util"
	"github.com/dolphindb/api-go/v3/logging"
)

func main() {
	util.InitExampleLogger()

	client, err := dolphindb.NewClient(
		context.Background(),
		apis.TestAddr,
		&dialer.BehaviorOptions{Reconnect: true},
	)
	if err != nil {
		logging.Error("example.client", "create client failed", "err", err)
		return
	}
	defer client.Close()

	if err := client.Connect(); err != nil {
		logging.Error("example.client", "connect failed", "err", err)
		return
	}

	loginReq := (&dolphindb.LoginRequest{}).
		SetUserID(apis.User).
		SetPassword(apis.Password)
	if err := client.Login(loginReq); err != nil {
		logging.Error("example.client", "login failed", "err", err)
		return
	}

	table, err := client.NewTable(
		(&dolphindb.NewTableRequest{}).
			SetTableName("t").
			AddTableParam("id", "1 2 3"),
	)
	if err != nil {
		logging.Error("example.client", "create table failed", "err", err)
		return
	}

	logging.Info("example.client", "new client table", "table", table.String())
}
