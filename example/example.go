package main

import (
	"github.com/dolphindb/api-go/v3/dolphindb"
	"github.com/dolphindb/api-go/v3/example/apis"
	"github.com/dolphindb/api-go/v3/example/util"
	"github.com/dolphindb/api-go/v3/logging"
)

func main() {
	util.InitExampleLogger()

	client, err := dolphindb.Dial(apis.TestAddr, apis.User, apis.Password, nil)
	if err != nil {
		logging.Error("example.quickstart", "dial failed", "err", err)
		return
	}
	defer client.Close()

	result, err := client.RunScript("1+1")
	if err != nil {
		logging.Error("example.quickstart", "run script failed", "err", err)
		return
	}

	logging.Info("example.quickstart", "dial quick start result", "result", result.String())
}
