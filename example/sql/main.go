package main

import (
	"context"

	"github.com/dolphindb/api-go/v3/api"
	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/dolphindb/api-go/v3/example/apis"
	"github.com/dolphindb/api-go/v3/example/util"
	"github.com/dolphindb/api-go/v3/logging"
)

func main() {
	util.InitExampleLogger()

	addr := apis.TestAddr
	user := apis.User
	password := apis.Password
	logging.Info("example.sql", "connecting to dolphindb", "address", addr)

	sql := "sysdate()"
	logging.Info("example.sql", "comparing sql standards", "sql", sql)

	runSQL(addr, user, password, dialer.SqlStdDolphinDB, sql)
	runSQL(addr, user, password, dialer.SqlStdOracle, sql)
	runSQL(addr, user, password, dialer.SqlStdMySQL, sql)

	logging.Info("example.sql", "sql standard comparison completed")
}

func runSQL(addr, user, password string, standard dialer.SqlStdEnum, sql string) {
	behavior := &dialer.BehaviorOptions{SqlStd: standard}
	logging.Info("example.sql", "running sql standard comparison", "standard", standard.String())
	expectedToFail := standard == dialer.SqlStdDolphinDB

	db, err := api.NewDolphinDBClient(context.TODO(), addr, behavior)
	if err != nil {
		logging.Error("example.sql", "create client failed", "standard", standard.String(), "err", err)
		return
	}

	err = db.Connect()
	if err != nil {
		logging.Error("example.sql", "connect failed", "standard", standard.String(), "err", err)
		return
	}
	defer func() {
		_ = db.Close()
	}()

	err = db.Login(new(api.LoginRequest).SetUserID(user).SetPassword(password))
	if err != nil {
		logging.Error("example.sql", "login failed", "standard", standard.String(), "err", err)
		return
	}

	df, err := db.RunScript(sql)
	if err != nil {
		if expectedToFail {
			logging.Info("example.sql", "run failed as expected", "standard", standard.String(), "err", err)
		} else {
			logging.Error("example.sql", "run failed", "standard", standard.String(), "err", err)
		}
		return
	}

	logging.Info("example.sql", "run succeeded", "standard", standard.String(), "result", df.String())
}
