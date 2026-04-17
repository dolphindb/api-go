package main

import (
	"context"
	"fmt"

	"github.com/dolphindb/api-go/v3/api"
	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/dolphindb/api-go/v3/example/apis"
)

func main() {
	addr := apis.TestAddr
	user := apis.User
	password := apis.Password
	fmt.Printf("Connecting to DolphinDB at %s\n", addr)

	sql := "sysdate()"
	fmt.Printf("Comparing SQL standards with SQL: %s\n\n", sql)

	runSQL(addr, user, password, dialer.SqlStdDolphinDB, sql)
	runSQL(addr, user, password, dialer.SqlStdOracle, sql)
	runSQL(addr, user, password, dialer.SqlStdMySQL, sql)

	fmt.Println("\nSQL standard comparison completed")
}

func runSQL(addr, user, password string, standard dialer.SqlStdEnum, sql string) {
	behavior := (&dialer.BehaviorOptions{}).SetSqlStd(standard)
	fmt.Printf("[%s]\n", standard.String())
	expectedToFail := standard == dialer.SqlStdDolphinDB

	db, err := api.NewDolphinDBClient(context.TODO(), addr, behavior)
	if err != nil {
		fmt.Printf("create client failed: %v\n\n", err)
		return
	}

	err = db.Connect()
	if err != nil {
		fmt.Printf("connect failed: %v\n\n", err)
		return
	}
	defer func() {
		_ = db.Close()
	}()

	err = db.Login(new(api.LoginRequest).SetUserID(user).SetPassword(password))
	if err != nil {
		fmt.Printf("login failed: %v\n\n", err)
		return
	}

	df, err := db.RunScript(sql)
	if err != nil {
		if expectedToFail {
			fmt.Printf("run failed as expected for %s: %v\n\n", standard.String(), err)
		} else {
			fmt.Printf("run failed: %v\n\n", err)
		}
		return
	}

	fmt.Printf("run succeeded: %s\n\n", df.String())
}
