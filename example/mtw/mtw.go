package mtw

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/dolphindb/api-go/dialer"
	"github.com/dolphindb/api-go/example/apis"
	"github.com/dolphindb/api-go/example/util"
	"github.com/dolphindb/api-go/model"
	mtw "github.com/dolphindb/api-go/multigoroutinetable"
)

// MultiGoroutineTable checks whether the MultiGoroutineTable is valid when inserts to memory table.
func MultiGoroutineTable() {
	conn, err := dialer.NewSimpleConn(context.TODO(), apis.TestAddr, apis.User, apis.Password)
	util.AssertNil(err)

	defer func() {
		_, _ = conn.RunScript("for(db in getClusterDFSDatabases()){\n	dropDatabase(db)\n}")
		_ = conn.Close()
	}()

	buf := bytes.NewBufferString("t=streamTable(1:0, `sym`tradeDate, [SYMBOL,DATEHOUR]);\n")
	buf.WriteString("addColumn(t,\"col\"+string(1..200),take([DOUBLE],200));share t as t1;\n")
	buf.WriteString("td=streamTable(1:0, `sym`tradeDate, [SYMBOL,DATEHOUR]);\n")
	buf.WriteString("addColumn(td,\"col\"+string(1..200),take([DOUBLE],200));share td as trades;")
	_, err = conn.RunScript(buf.String())
	util.AssertNil(err)

	opt := &mtw.Option{
		Database:       "",
		Address:        apis.TestAddr,
		UserID:         apis.User,
		Password:       apis.Password,
		TableName:      "trades",
		GoroutineCount: 2,
		PartitionCol:   "sym",
		BatchSize:      10000,
		Throttle:       1,
	}

	mtt, err := mtw.NewMultiGoroutineTable(opt)
	util.AssertNil(err)

	for ind := 0; ind < 10000; ind++ {
		row := make([]model.DataForm, 202)
		dt, err := model.NewDataType(model.DtString, "2")
		util.AssertNil(err)

		row[0] = model.NewScalar(dt)

		dt, err = model.NewDataType(model.DtNanoTimestamp, time.Date(2022, time.Month(1), 1+ind%10, 1, 1, 0, 0, time.UTC))
		util.AssertNil(err)

		row[1] = model.NewScalar(dt)
		i := float64(ind)
		for j := 0; j < 200; j++ {
			dt, err = model.NewDataType(model.DtDouble, i+0.1)
			util.AssertNil(err)

			row[j+2] = model.NewScalar(dt)
		}

		_, err = conn.RunFunc("tableInsert{t1}", row)
		util.AssertNil(err)

		err = mtt.Insert("2", time.Date(2022, time.Month(1), 1+ind%10, 1, 1, 0, 0, time.UTC), i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
			i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
			i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
			i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
			i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
			i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
			i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1)
		util.AssertNil(err)
	}

	mtt.WaitForGoroutineCompletion()
	raw, err := conn.RunScript("select * from trades order by sym,tradeDate;")
	util.AssertNil(err)

	bt := raw.(*model.Table)

	raw, err = conn.RunScript("select * from t1 order by sym,tradeDate;")
	util.AssertNil(err)

	ex := raw.(*model.Table)
	for k := range bt.ColNames {
		col1 := bt.GetColumnByIndex(k)
		col2 := ex.GetColumnByIndex(k)
		util.AssertEqual(col1.String(), col2.String())
	}

	fmt.Println("Run MultiGoroutineTable successful")
}

// MultiGoroutineDfsTable checks whether the MultiGoroutineTable is valid when inserts to dfs table.
func MultiGoroutineDfsTable() {
	conn, err := dialer.NewSimpleConn(context.TODO(), apis.TestAddr, apis.User, apis.Password)
	util.AssertNil(err)

	defer func() {
		_, _ = conn.RunScript("for(db in getClusterDFSDatabases()){\n	dropDatabase(db)\n}")
		_ = conn.Close()
	}()

	buf := bytes.NewBufferString("t=table(1:0, `sym`tradeDate, [SYMBOL,TIMESTAMP]);\n")
	buf.WriteString("addColumn(t,\"col\"+string(1..200),take([DOUBLE],200));share t as t1;\n")
	buf.WriteString("dbName = \"dfs://test_MultigoroutineTableWriter\"\n")
	buf.WriteString("if(exists(dbName)){\n")
	buf.WriteString("	dropDatabase(dbName)	\n")
	buf.WriteString("}\n")
	buf.WriteString("db=database(dbName, VALUE, date(1..2),,'TSDB');\n")
	buf.WriteString("createPartitionedTable(dbHandle=db, table=t, tableName=`pt1, partitionColumns=[\"tradeDate\"],sortColumns=`tradeDate,compressMethods={tradeDate:\"delta\"});")
	_, err = conn.RunScript(buf.String())
	util.AssertNil(err)

	opt := &mtw.Option{
		Database:       "dfs://test_MultigoroutineTableWriter",
		Address:        apis.TestAddr,
		UserID:         apis.User,
		Password:       apis.Password,
		TableName:      "pt1",
		GoroutineCount: 2,
		PartitionCol:   "tradeDate",
		BatchSize:      10000,
		Throttle:       1,
	}

	mtt, err := mtw.NewMultiGoroutineTable(opt)
	util.AssertNil(err)

	for ind := 0; ind < 10000; ind++ {
		row := make([]model.DataForm, 202)
		dt, err := model.NewDataType(model.DtString, "2")
		util.AssertNil(err)

		row[0] = model.NewScalar(dt)

		dt, err = model.NewDataType(model.DtNanoTimestamp, time.Date(2022, time.Month(1), 1+ind%10, 1, 1, 0, 0, time.UTC))
		util.AssertNil(err)

		row[1] = model.NewScalar(dt)
		i := float64(ind)
		for j := 0; j < 200; j++ {
			dt, err = model.NewDataType(model.DtDouble, i+0.1)
			util.AssertNil(err)

			row[j+2] = model.NewScalar(dt)
		}

		_, err = conn.RunFunc("tableInsert{t1}", row)
		util.AssertNil(err)

		err = mtt.Insert("2", time.Date(2022, time.Month(1), 1+ind%10, 1, 1, 0, 0, time.UTC), i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
			i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
			i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
			i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
			i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
			i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
			i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1)
		util.AssertNil(err)
	}

	mtt.WaitForGoroutineCompletion()
	raw, err := conn.RunScript("select * from loadTable('dfs://test_MultigoroutineTableWriter',`pt1) order by sym,tradeDate;")
	util.AssertNil(err)

	bt := raw.(*model.Table)

	raw, err = conn.RunScript("select * from t1 order by sym,tradeDate;")
	util.AssertNil(err)

	ex := raw.(*model.Table)
	for k := range bt.ColNames {
		col1 := bt.GetColumnByIndex(k)
		col2 := ex.GetColumnByIndex(k)
		util.AssertEqual(col1.String(), col2.String())
	}

	fmt.Println("Run MultiGoroutineedDfsTable successful")
}
