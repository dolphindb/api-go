package main

import (
	"github.com/dolphindb/api-go/v3/dolphindb"
	"github.com/dolphindb/api-go/v3/example/apis"
	"github.com/dolphindb/api-go/v3/example/util"
	"github.com/dolphindb/api-go/v3/logging"
)

func main() {
	util.InitExampleLogger()

	db, err := dolphindb.Dial(apis.TestAddr, apis.User, apis.Password, nil)
	if err != nil {
		logging.Error("example.logging", "dial failed", "err", err)
		return
	}
	defer db.Close()

	if _, err := db.RunScript("1+1"); err != nil {
		logging.Error("example.logging", "run script failed", "err", err)
		return
	}

	logging.Info("example.logging", "run script succeeded")
}
