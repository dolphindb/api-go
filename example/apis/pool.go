package apis

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/dialer"
	"github.com/dolphindb/api-go/example/util"
	"github.com/dolphindb/api-go/model"
)

func dropDatabase(db api.DolphinDB, dbPath string) {
	script := bytes.NewBufferString(fmt.Sprintf("if(existsDatabase(\"%s\")){\n", dbPath))
	script.WriteString(fmt.Sprintf("	dropDatabase(\"%s\")\n", dbPath))
	script.WriteString("}\n")
	_, err := db.RunScript(script.String())
	util.AssertNil(err)
}

// PartitionedTableAppenderWithValueDomain checks whether the PartitionedTableAppender is valid with value domain.
func PartitionedTableAppenderWithValueDomain(db api.DolphinDB) {
	script := bytes.NewBufferString("t = table(timestamp(1..10)  as date,string(1..10) as sym)\n")
	script.WriteString("db1=database(\"\",HASH,[DATETIME,10])\n")
	script.WriteString("db2=database(\"\",VALUE,string(1..10))\n")
	script.WriteString("if(existsDatabase(\"dfs://demohash\")){\n")
	script.WriteString("	dropDatabase(\"dfs://demohash\")\n")
	script.WriteString("}\n")
	script.WriteString("db =database(\"dfs://demohash\",COMPO,[db1,db2])\n")
	script.WriteString("pt = db.createPartitionedTable(t,`pt,`date`sym)\n")

	_, err := db.RunScript(script.String())
	util.AssertNil(err)

	defer dropDatabase(db, "dfs://demohash")

	poolOpt := &api.PoolOption{
		Address:  TestAddr,
		UserID:   User,
		Password: Password,
		PoolSize: 3,
	}

	pool, err := api.NewDBConnectionPool(poolOpt)
	util.AssertNil(err)

	appenderOpt := &api.PartitionedTableAppenderOption{
		Pool:         pool,
		DBPath:       "dfs://demohash",
		TableName:    "pt",
		PartitionCol: "sym",
	}

	appender, err := api.NewPartitionedTableAppender(appenderOpt)
	util.AssertNil(err)

	colNames := []string{"date", "sym"}
	cols := make([]*model.Vector, 2)

	times := make([]time.Time, 10000)
	for i := 0; i < 10000; i++ {
		times[i] = time.Now()
	}

	l, err := model.NewDataTypeListWithRaw(model.DtTimestamp, times)
	util.AssertNil(err)

	cols[0] = model.NewVector(l)

	sym := make([]string, 10000)
	for i := 0; i < 10000; i += 4 {
		sym[i] = "2"
		sym[i+1] = "3"
		sym[i+2] = "4"
		sym[i+3] = "5"
	}

	l, err = model.NewDataTypeListWithRaw(model.DtString, sym)
	util.AssertNil(err)

	cols[1] = model.NewVector(l)
	for i := 0; i < 1000; i++ {
		m, err := appender.Append(model.NewTable(colNames, cols))
		util.AssertNil(err)
		util.AssertEqual(m, 10000)
	}

	err = appender.Close()
	util.AssertNil(err)

	df, err := db.RunScript("pt= loadTable(\"dfs://demohash\",`pt)\nexec count(*) from pt")
	util.AssertNil(err)
	util.AssertEqual(df.String(), "long(10000000)")

	fmt.Println("Run PartitionedTableAppenderWithValueDomain successful")
}

// PartitionedTableAppenderWithHashDomain checks whether the PartitionedTableAppender is valid with hash domain.
func PartitionedTableAppenderWithHashDomain(db api.DolphinDB) {
	script := bytes.NewBufferString("t = table(timestamp(1..10)  as date,int(1..10) as sym)\n")
	script.WriteString("db1=database(\"\",HASH,[DATETIME,10])\n")
	script.WriteString("db2=database(\"\",HASH,[INT,5])\n")
	script.WriteString("if(existsDatabase(\"dfs://demohash\")){\n")
	script.WriteString("	dropDatabase(\"dfs://demohash\")}\n")
	script.WriteString("db =database(\"dfs://demohash\",COMPO,[db2,db1])\n")
	script.WriteString("pt = db.createPartitionedTable(t,`pt,`sym`date)")

	_, err := db.RunScript(script.String())
	util.AssertNil(err)

	defer dropDatabase(db, "dfs://demohash")

	poolOpt := &api.PoolOption{
		Address:  TestAddr,
		UserID:   User,
		Password: Password,
		PoolSize: 3,
	}

	pool, err := api.NewDBConnectionPool(poolOpt)
	util.AssertNil(err)

	appenderOpt := &api.PartitionedTableAppenderOption{
		Pool:         pool,
		DBPath:       "dfs://demohash",
		TableName:    "pt",
		PartitionCol: "sym",
	}

	appender, err := api.NewPartitionedTableAppender(appenderOpt)
	util.AssertNil(err)

	colNames := []string{"date", "sym"}
	cols := make([]*model.Vector, 2)

	times := make([]time.Time, 10000)
	for i := 0; i < 10000; i++ {
		times[i] = time.Date(2020, time.Month(5), 06, 21, 01, 48, 200, time.UTC)
	}

	l, err := model.NewDataTypeListWithRaw(model.DtTimestamp, times)
	util.AssertNil(err)

	cols[0] = model.NewVector(l)

	sym := make([]int32, 10000)
	for i := 0; i < 10000; i += 4 {
		sym[i] = int32(1)
		sym[i+1] = int32(23)
		sym[i+2] = int32(325)
		sym[i+3] = int32(11)
	}

	l, err = model.NewDataTypeListWithRaw(model.DtInt, sym)
	util.AssertNil(err)

	cols[1] = model.NewVector(l)
	for i := 0; i < 1000; i++ {
		m, err := appender.Append(model.NewTable(colNames, cols))
		util.AssertNil(err)
		util.AssertEqual(m, 10000)
	}

	err = appender.Close()
	util.AssertNil(err)

	df, err := db.RunScript("pt= loadTable(\"dfs://demohash\",`pt)\nexec count(*) from pt")
	util.AssertNil(err)
	util.AssertEqual(df.String(), "long(10000000)")

	fmt.Println("Run PartitionedTableAppenderWithHashDomain successful")
}

// TableAppender checks whether the TableAppender is valid.
func TableAppender(db api.DolphinDB) {
	script := bytes.NewBufferString("dbPath = \"dfs://tableAppenderTest\"\n")
	script.WriteString("if(existsDatabase(dbPath)){\n")
	script.WriteString("	dropDatabase(dbPath)\n")
	script.WriteString("}\n")
	script.WriteString("t = table(100:0,`id`time`data,[INT,TIME,DOUBLE])\n")
	script.WriteString("db=database(dbPath,HASH, [INT,10])\n")
	script.WriteString("pt = db.createPartitionedTable(t,`testAppend,`id)\n")

	_, err := db.RunScript(script.String())
	util.AssertNil(err)

	defer dropDatabase(db, "dfs://tableAppenderTest")

	conn, err := dialer.NewSimpleConn(context.TODO(), TestAddr, User, Password)
	util.AssertNil(err)

	opt := &api.TableAppenderOption{
		DBPath:    "dfs://tableAppenderTest",
		TableName: "testAppend",
		Conn:      conn,
	}

	appender := api.NewTableAppender(opt)
	util.AssertNil(err)

	tb := packTable()
	_, err = appender.Append(tb)
	util.AssertNil(err)

	df, err := db.RunScript("exec count(*) from loadTable(\"dfs://tableAppenderTest\", \"testAppend\")")
	util.AssertNil(err)
	util.AssertEqual(df.String(), "long(100000)")

	fmt.Println("Run TableAppender successful")
}

func packTable() *model.Table {
	size := 100000
	id := make([]int32, size)
	data := make([]float64, size)
	ts := make([]time.Time, size)
	for i := 0; i < size; i++ {
		ts[i] = time.Now()
		id[i] = rand.Int31()
		data[i] = rand.Float64()
	}

	dtl, err := model.NewDataTypeListWithRaw(model.DtInt, id)
	util.AssertNil(err)

	idVct := model.NewVector(dtl)

	dtl, err = model.NewDataTypeListWithRaw(model.DtDouble, data)
	util.AssertNil(err)

	dataVct := model.NewVector(dtl)

	dtl, err = model.NewDataTypeListWithRaw(model.DtTimestamp, ts)
	util.AssertNil(err)

	timeVct := model.NewVector(dtl)

	return model.NewTable([]string{"id", "time", "data"}, []*model.Vector{idVct, timeVct, dataVct})
}
