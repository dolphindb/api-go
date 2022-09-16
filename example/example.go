package main

import (
	"context"
	"fmt"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/example/apis"
	"github.com/dolphindb/api-go/example/mtw"
	"github.com/dolphindb/api-go/example/script"
	"github.com/dolphindb/api-go/example/streaming_test"
	"github.com/dolphindb/api-go/example/util"
)

func main() {
	// new a DolphinDB client
	db, err := api.NewDolphinDBClient(context.TODO(), apis.TestAddr, nil)
	util.AssertNil(err)

	// connect to server
	err = db.Connect()
	util.AssertNil(err)

	// close connection
	defer db.Close()

	// log in server
	err = apis.Login(db)
	util.AssertNil(err)

	// check whether the database exists
	err = apis.ExistsDatabase(db)
	util.AssertNil(err)

	// create database
	_, err = apis.Database(db)
	util.AssertNil(err)

	// check whether the database exists
	err = apis.ExistsDatabase(db)
	util.AssertNil(err)

	// test memory table
	_, err = apis.Table(db)
	util.AssertNil(err)
	err = apis.SaveTable(db)
	util.AssertNil(err)
	err = apis.SaveText(db)
	util.AssertNil(err)
	err = apis.LoadText(db)
	util.AssertNil(err)
	err = apis.PloadText(db)
	util.AssertNil(err)
	err = apis.LoadTable(db)
	util.AssertNil(err)
	err = apis.DropTable(db)
	util.AssertNil(err)

	err = apis.DropDatabase(db)
	util.AssertNil(err)
	err = apis.ExistsDatabase(db)
	util.AssertNil(err)

	// test partitioned table
	sd, err := apis.SegmentDatabase(db)
	util.AssertNil(err)

	cta, err := apis.TableWithCapacity(db)
	util.AssertNil(err)
	dt, err := apis.CreatePartitionedTable(sd)
	util.AssertNil(err)

	_, err = db.RunScript(fmt.Sprintf("%s.append!(%s)", dt.Handle, cta.Handle))
	util.AssertNil(err)

	err = apis.LoadPartitionedTable(db)
	util.AssertNil(err)
	err = apis.LoadTableBySQL(db, dt.Handle)
	util.AssertNil(err)
	// err = apis.DropPartition(db)
	err = apis.DropPartitionTable(db)
	util.AssertNil(err)
	_, err = apis.CreateTable(sd)
	util.AssertNil(err)

	err = apis.DropSegmentTable(db)
	util.AssertNil(err)
	err = apis.DropSegmentDatabase(db)
	util.AssertNil(err)

	// check script,function and upload func
	script.CheckDataType(db)
	script.CheckDataForm(db)
	script.CheckFunction(db)

	// test Appender
	apis.PartitionedTableAppenderWithValueDomain(db)
	apis.PartitionedTableAppenderWithHashDomain(db)
	apis.TableAppender(db)

	// test streaming
	streaming_test.GoroutineClient(db)
	streaming_test.PollingClient(db)
	streaming_test.GoroutinePooledClient(db)

	//test mtw
	mtw.MultiGoroutineDfsTable()
	mtw.MultiGoroutineTable()

	// clear cache
	err = db.UndefAll()
	util.AssertNil(err)
	c := new(api.ClearAllCacheRequest).SetIsDFS(true)
	err = db.ClearAllCache(c)
	util.AssertNil(err)
	err = apis.Logout(db)
	util.AssertNil(err)
}
