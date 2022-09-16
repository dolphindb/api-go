package test

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

var dbconnPool, _ = api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)

func CheckConnectionPool(OriginConnectionNum []interface{}, NewConnectionNum []interface{}) bool {
	for i := 0; i < len(OriginConnectionNum); i++ {
		if OriginConnectionNum[i].(int32) >= NewConnectionNum[i].(int32) {
			return false
		}
	}
	return true
}

func WaitConnectionPoolSuccess(OriginConnectionNum []interface{}) bool {
	for {
		NewConnectionNum := GetConnectionNum()
		fmt.Println(NewConnectionNum)
		res := CheckConnectionPool(OriginConnectionNum, NewConnectionNum)
		if res == true {
			break
		}
		time.Sleep(3 * time.Second)
		continue
	}
	return true
}

func GetConnectionNum() []interface{} {
	Table, _ := dbconnPool.RunScript("select connectionNum, name from rpc(getControllerAlias(), getClusterPerf) where mode = 0")
	tmpTable := Table.(*model.Table)
	connectionNumList := tmpTable.GetColumnByName(tmpTable.GetColumnNames()[0])
	connectionNum := connectionNumList.Data.Value()
	return connectionNum
}

func GetOriginConnNum() []interface{} {
	var OriginConnectionNum []interface{}
	var i = 0
	for {
		OriginConnectionNum = GetConnectionNum()
		if OriginConnectionNum != nil && i == 9 {
			break
		}
		i++
	}
	time.Sleep(3 * time.Second)
	OriginConnectionNum = GetConnectionNum()
	return OriginConnectionNum
}

func CheckConnectionNum(OriginConnectionNum []interface{}) bool {
	Table, _ := dbconnPool.RunScript("select connectionNum, name from rpc(getControllerAlias(), getClusterPerf) where mode = 0")
	tmpTable := Table.(*model.Table)
	connectionNumList := tmpTable.GetColumnByName(tmpTable.GetColumnNames()[0])
	connectionNum := connectionNumList.Data.Value()
	fmt.Printf("\nNewConnection:%v\n", connectionNum)
	for i := 0; i < connectionNumList.Rows(); i++ {
		if OriginConnectionNum[i].(int32) >= connectionNum[i].(int32) {
			return false
		}
		for j := i; j < connectionNumList.Rows(); j++ {
			if (connectionNum[j].(int32)-OriginConnectionNum[j].(int32))-(connectionNum[i].(int32)-OriginConnectionNum[i].(int32)) > 2 || (connectionNum[j].(int32)-OriginConnectionNum[j].(int32))-(connectionNum[i].(int32)-OriginConnectionNum[i].(int32)) < -2 {
				return false
			}
		}
	}
	return true
}

func TestDBConnectionPool_exception(t *testing.T) {
	Convey("Test_function_DBConnectionPool_exception_test", t, func() {
		Convey("Test_function_DBConnectionPool_wrong_address_exception \n", func() {
			opt := &api.PoolOption{
				Address:     "129.16.12.14",
				UserID:      setup.UserName,
				Password:    setup.Password,
				PoolSize:    2,
				LoadBalance: false,
			}
			_, err := api.NewDBConnectionPool(opt)
			So(err, ShouldNotBeNil)
		})
		Convey("Test_function_DBConnectionPool_address_nil_exception \n", func() {
			opt := &api.PoolOption{
				Address:     "",
				UserID:      setup.UserName,
				Password:    setup.Password,
				PoolSize:    2,
				LoadBalance: false,
			}
			_, err := api.NewDBConnectionPool(opt)
			So(err, ShouldNotBeNil)
		})
		Convey("Test_function_DBConnectionPool_wrong_userName_exception \n", func() {
			opt := &api.PoolOption{
				Address:     setup.Address,
				UserID:      "rootn1",
				Password:    setup.Password,
				PoolSize:    2,
				LoadBalance: false,
			}
			_, err := api.NewDBConnectionPool(opt)
			So(err, ShouldNotBeNil)
		})
		Convey("Test_function_DBConnectionPool_userName_null_exception \n", func() {
			opt := &api.PoolOption{
				Address:     setup.Address,
				UserID:      "",
				Password:    setup.Password,
				PoolSize:    2,
				LoadBalance: false,
			}
			_, err := api.NewDBConnectionPool(opt)
			So(err, ShouldNotBeNil)
		})
		Convey("Test_function_DBConnectionPool_wrong_Password_exception \n", func() {
			opt := &api.PoolOption{
				Address:     setup.Address,
				UserID:      setup.UserName,
				Password:    "rpoot120@",
				PoolSize:    2,
				LoadBalance: false,
			}
			_, err := api.NewDBConnectionPool(opt)
			So(err, ShouldNotBeNil)
		})
		Convey("Test_function_DBConnectionPool_wrong_Password_special_symbol_exception \n", func() {
			opt := &api.PoolOption{
				Address:     setup.Address,
				UserID:      setup.UserName,
				Password:    "!!!!!",
				PoolSize:    2,
				LoadBalance: false,
			}
			_, err := api.NewDBConnectionPool(opt)
			So(err, ShouldNotBeNil)
		})
		Convey("Test_function_DBConnectionPool_PoolSize_less_than_0_exception", func() {
			opt := &api.PoolOption{
				Address:     setup.Address,
				UserID:      setup.UserName,
				Password:    setup.Password,
				PoolSize:    -1,
				LoadBalance: false,
			}
			_, err := api.NewDBConnectionPool(opt)
			So(err, ShouldNotBeNil)
		})
		Convey("Test_function_DBConnectionPool_SetLoadBalanceAddress_LoadBalance_false_exception", func() {
			OriginConnectionNum := GetOriginConnNum()
			fmt.Printf("\norigin connection:%v\n", OriginConnectionNum)
			opt := &api.PoolOption{
				Address:              setup.Address,
				UserID:               setup.UserName,
				Password:             setup.Password,
				PoolSize:             5,
				LoadBalance:          false,
				LoadBalanceAddresses: []string{setup.Address, setup.Address2, setup.Address3, setup.Address4},
			}
			pool, err := api.NewDBConnectionPool(opt)
			So(err, ShouldBeNil)
			re := pool.GetPoolSize()
			So(re, ShouldEqual, 5)
			closed := pool.IsClosed()
			So(closed, ShouldBeFalse)
			err = pool.Close()
			So(err, ShouldBeNil)
			closed = pool.IsClosed()
			So(closed, ShouldBeTrue)
		})
	})
}
func TestDBConnectionPool_Execute(t *testing.T) {
	Convey("Test_function_DBConnectionPool_Execute", t, func() {
		opt := &api.PoolOption{
			Address:     setup.Address,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    2,
			LoadBalance: false,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		re := pool.GetPoolSize()
		So(re, ShouldEqual, 2)
		dt, err := model.NewDataType(model.DtString, "true")
		So(err, ShouldBeNil)
		s := model.NewScalar(dt)
		task := &api.Task{
			Script: "typestr",
			Args:   []model.DataForm{s},
		}
		err = pool.Execute([]*api.Task{task, task, task})
		So(err, ShouldBeNil)
		err = task.GetError()
		So(err, ShouldBeNil)
		closed := pool.IsClosed()
		So(closed, ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		closed = pool.IsClosed()
		So(closed, ShouldBeTrue)
	})
}
func TestDBConnectionPool_LoadBalance(t *testing.T) {
	Convey("Test_function_DBConnectionPool_LoadBalance_true", t, func() {
		OriginConnectionNum := GetOriginConnNum()
		fmt.Printf("\norigin connection:%v\n", OriginConnectionNum)
		opt := &api.PoolOption{
			Address:     setup.Address,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    5,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		re := pool.GetPoolSize()
		So(re, ShouldEqual, 5)
		IsSucess := WaitConnectionPoolSuccess(OriginConnectionNum)
		So(IsSucess, ShouldBeTrue)
		connBalance := CheckConnectionNum(OriginConnectionNum)
		So(connBalance, ShouldBeTrue)
		closed := pool.IsClosed()
		So(closed, ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		closed = pool.IsClosed()
		So(closed, ShouldBeTrue)
	})
}
func TestDBConnectionPool_SetLoadBalanceAddress(t *testing.T) {
	Convey("Test_function_DBConnectionPool_SetLoadBalanceAddress", t, func() {
		time.Sleep(3 * time.Second)
		OriginConnectionNum := GetConnectionNum()
		fmt.Printf("\norigin connection:%v\n", OriginConnectionNum)
		opt := &api.PoolOption{
			Address:              setup.Address,
			UserID:               setup.UserName,
			Password:             setup.Password,
			PoolSize:             5,
			LoadBalance:          true,
			LoadBalanceAddresses: []string{setup.Address, setup.Address2, setup.Address3, setup.Address4},
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		re := pool.GetPoolSize()
		So(re, ShouldEqual, 5)
		IsSucess := WaitConnectionPoolSuccess(OriginConnectionNum)
		So(IsSucess, ShouldBeTrue)
		connBalance := CheckConnectionNum(OriginConnectionNum)
		So(connBalance, ShouldBeTrue)
		closed := pool.IsClosed()
		So(closed, ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		closed = pool.IsClosed()
		So(closed, ShouldBeTrue)
	})
}

func TestDBConnectionPool_hash_hash_string(t *testing.T) {
	Convey("TestDBConnectionPool_hash_hash_string", t, func() {
		_, err := dbconnPool.RunScript("t = table(timestamp(1..10) as datev,string(1..10) as sym)\n" +
			"db1=database(\"\",HASH,[DATETIME,10])\n" +
			"db2=database(\"\",HASH,[STRING,5])\n" +
			"if(existsDatabase(\"dfs://demohash\")){\n" +
			"\tdropDatabase(\"dfs://demohash\")}\n" +
			"db=database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
			"pt=db.createPartitionedTable(t,`pt,`sym`datev)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     setup.Address,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://demohash",
			TableName:    "pt",
			PartitionCol: "sym",
		}
		appender, err := api.NewPartitionedTableAppender(appenderOpt)
		So(err, ShouldBeNil)
		var symarr []string
		var datetimearr []time.Time
		for i := 0; i < 10000; i++ {
			symarr = append(symarr, strconv.Itoa(i))
			datetimearr = append(datetimearr, time.Date(1969, time.Month(12), i, 23, i, 50, 000, time.UTC))
		}
		sym, err := model.NewDataTypeListWithRaw(model.DtString, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListWithRaw(model.DtTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := dbconnPool.RunScript("pt= loadTable(\"dfs://demohash\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_value_hash_symbol(t *testing.T) {
	Convey("TestDBConnectionPool_value_hash_symbol", t, func() {
		_, err := dbconnPool.RunScript("t = table(timestamp(1..10) as datev,string(1..10) as sym)\n" +
			"db1=database(\"\",VALUE,date(2022.01.01)+0..100)\n" +
			"db2=database(\"\",HASH,[STRING,5])\n" +
			"if(existsDatabase(\"dfs://demohash\")){\n" +
			"\tdropDatabase(\"dfs://demohash\")}\n" +
			"db=database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
			"pt=db.createPartitionedTable(t,`pt,`sym`datev)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     setup.Address,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://demohash",
			TableName:    "pt",
			PartitionCol: "sym",
		}
		appender, err := api.NewPartitionedTableAppender(appenderOpt)
		So(err, ShouldBeNil)
		var symarr []string
		var datetimearr []time.Time
		for i := 0; i < 10000; i++ {
			symarr = append(symarr, strconv.Itoa(i))
			datetimearr = append(datetimearr, time.Date(2022, time.Month(01), i, 23, 12, 50, 000, time.UTC))
		}
		sym, err := model.NewDataTypeListWithRaw(model.DtString, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListWithRaw(model.DtTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := dbconnPool.RunScript("pt= loadTable(\"dfs://demohash\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_hash_hash_int(t *testing.T) {
	Convey("TestDBConnectionPool_hash_hash_int", t, func() {
		_, err := dbconnPool.RunScript("t = table(timestamp(1..10) as datev,1..10 as sym)\n" +
			"db1=database(\"\",HASH,[DATETIME,10])\n" +
			"db2=database(\"\",HASH,[INT,5])\n" +
			"if(existsDatabase(\"dfs://demohash\")){\n" +
			"\tdropDatabase(\"dfs://demohash\")}\n" +
			"db=database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
			"pt=db.createPartitionedTable(t,`pt,`sym`datev)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     setup.Address,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://demohash",
			TableName:    "pt",
			PartitionCol: "sym",
		}
		appender, err := api.NewPartitionedTableAppender(appenderOpt)
		So(err, ShouldBeNil)
		var symarr []int32
		var datetimearr []time.Time
		for i := 0; i < 10000; i++ {
			symarr = append(symarr, int32(i))
			datetimearr = append(datetimearr, time.Date(1969, time.Month(12), i, 23, i, 50, 000, time.UTC))
		}
		sym, err := model.NewDataTypeListWithRaw(model.DtInt, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListWithRaw(model.DtTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := dbconnPool.RunScript("pt= loadTable(\"dfs://demohash\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_value_hash_datetime(t *testing.T) {
	Convey("TestDBConnectionPool_value_hash_datetime", t, func() {
		_, err := dbconnPool.RunScript("\n" +
			"t = table(datetime(1..10) as datev,string(1..10) as sym)\n" +
			"db2=database(\"\",VALUE,string(0..10))\n" +
			"db1=database(\"\",HASH,[DATETIME,10])\n" +
			"if(existsDatabase(\"dfs://demohash\")){\n" +
			"\tdropDatabase(\"dfs://demohash\")\n" +
			"}\n" +
			"db=database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
			"pt=db.createPartitionedTable(t,`pt,`sym`datev)\n")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     setup.Address,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://demohash",
			TableName:    "pt",
			PartitionCol: "sym",
		}
		appender, err := api.NewPartitionedTableAppender(appenderOpt)
		So(err, ShouldBeNil)
		var symarr []string
		var datetimearr []time.Time
		rand.Seed(time.Now().Unix())
		for i := 0; i < 10000; i++ {
			rand.Seed(time.Now().Unix())
			symarr = append(symarr, strconv.Itoa(rand.Intn(10)))
			datetimearr = append(datetimearr, time.Date(2022, time.Month(01), 1+i, 23, 12, 50, 000, time.UTC))
		}
		sym, err := model.NewDataTypeListWithRaw(model.DtString, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListWithRaw(model.DtDatetime, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := dbconnPool.RunScript("pt= loadTable(\"dfs://demohash\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_range_hash_date(t *testing.T) {
	Convey("TestDBConnectionPool_range_hash_date", t, func() {
		_, err := dbconnPool.RunScript("t = table(date(1..10) as datev,symbol(string(1..10)) as sym)\n" +
			"db1=database(\"\",RANGE,date([0, 5, 11]))\n" +
			"db2=database(\"\",HASH,[SYMBOL,15])\n" +
			"if(existsDatabase(\"dfs://demohash\")){\n" +
			"\tdropDatabase(\"dfs://demohash\")\n" +
			"}\n" +
			"db=database(\"dfs://demohash\",COMPO,[db1,db2])\n" +
			"pt=db.createPartitionedTable(t,`pt,`datev`sym)\n")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     setup.Address,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://demohash",
			TableName:    "pt",
			PartitionCol: "sym",
		}
		appender, err := api.NewPartitionedTableAppender(appenderOpt)
		So(err, ShouldBeNil)
		var symarr []string
		var datetimearr []time.Time
		rand.Seed(time.Now().Unix())
		for i := 0; i < 10000; i++ {
			rand.Seed(time.Now().Unix())
			symarr = append(symarr, strconv.Itoa(rand.Intn(10)))
			datetimearr = append(datetimearr, time.Date(1970, time.Month(01), 3, 23, 12, 50, 000, time.UTC))
		}
		sym, err := model.NewDataTypeListWithRaw(model.DtSymbol, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListWithRaw(model.DtDate, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := dbconnPool.RunScript("pt= loadTable(\"dfs://demohash\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_range_range_int(t *testing.T) {
	Convey("TestDBConnectionPool_range_range_int", t, func() {
		_, err := dbconnPool.RunScript("\n" +
			"t = table(nanotimestamp(1..10) as datev, 1..10 as sym)\n" +
			"db1=database(\"\",RANGE,date(1970.01.01)+0..100*5)\n" +
			"db2=database(\"\",RANGE,0 2 4 6 8 11)\n" +
			"if(existsDatabase(\"dfs://demohash\")){\n" +
			"\tdropDatabase(\"dfs://demohash\")\n" +
			"}\n" +
			"db =database(\"dfs://demohash\",COMPO,[db1,db2])\n" +
			"pt = db.createPartitionedTable(t,`pt,`datev`sym)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     setup.Address,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://demohash",
			TableName:    "pt",
			PartitionCol: "sym",
		}
		appender, err := api.NewPartitionedTableAppender(appenderOpt)
		So(err, ShouldBeNil)
		var symarr []int32
		var datetimearr []time.Time
		rand.Seed(time.Now().Unix())
		for i := 0; i < 10000; i++ {
			symarr = append(symarr, int32(rand.Intn(10)))
			rand.Seed(time.Now().Unix())
			datetimearr = append(datetimearr, time.Date(1970, time.Month(01), 1+rand.Intn(300), 23, 12, 50, 789456478, time.UTC))
		}
		sym, err := model.NewDataTypeListWithRaw(model.DtInt, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListWithRaw(model.DtNanoTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := dbconnPool.RunScript("pt= loadTable(\"dfs://demohash\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_value_range_int(t *testing.T) {
	Convey("TestDBConnectionPool_value_range_int", t, func() {
		_, err := dbconnPool.RunScript("\n" +
			"t = table(timestamp(1..10) as datev,1..10 as sym)\n" +
			"db1=database(\"\",VALUE,date(1970.01.01)+0..10)\n" +
			"db2=database(\"\",RANGE,0 2 4 6 8 11)\n" +
			"if(existsDatabase(\"dfs://demohash\")){\n" +
			"\tdropDatabase(\"dfs://demohash\")\n" +
			"}\n" +
			"db =database(\"dfs://demohash\",COMPO,[db1,db2])\n" +
			"pt = db.createPartitionedTable(t,`pt,`datev`sym)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     setup.Address,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://demohash",
			TableName:    "pt",
			PartitionCol: "sym",
		}
		appender, err := api.NewPartitionedTableAppender(appenderOpt)
		So(err, ShouldBeNil)
		var symarr []int32
		var datetimearr []time.Time
		rand.Seed(time.Now().Unix())
		for i := 0; i < 10000; i++ {
			symarr = append(symarr, int32(rand.Intn(10)))
			rand.Seed(time.Now().Unix())
			datetimearr = append(datetimearr, time.Date(1970, time.Month(01), 1+rand.Intn(10), 23, 12, 50, 789456478, time.UTC))
		}
		sym, err := model.NewDataTypeListWithRaw(model.DtInt, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListWithRaw(model.DtTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := dbconnPool.RunScript("pt= loadTable(\"dfs://demohash\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_range_range_month(t *testing.T) {
	Convey("TestDBConnectionPool_range_range_month", t, func() {
		_, err := dbconnPool.RunScript("\n" +
			"t = table(nanotimestamp(1..10) as datev,1..10 as sym)\n" +
			"db2=database(\"\",RANGE,0 2 4 6 8 11)\n" +
			"db1=database(\"\",RANGE,month(1970.01M)+0..100*5)\n" +
			"if(existsDatabase(\"dfs://demohash\")){\n" +
			"\tdropDatabase(\"dfs://demohash\")\n" +
			"}\n" +
			"db =database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
			"pt = db.createPartitionedTable(t,`pt,`sym`datev)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     setup.Address,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://demohash",
			TableName:    "pt",
			PartitionCol: "sym",
		}
		appender, err := api.NewPartitionedTableAppender(appenderOpt)
		So(err, ShouldBeNil)
		var symarr []int32
		var datetimearr []time.Time
		rand.Seed(time.Now().Unix())
		for i := 0; i < 10000; i++ {
			symarr = append(symarr, int32(rand.Intn(10)))
			rand.Seed(time.Now().Unix())
			datetimearr = append(datetimearr, time.Date(1970, time.Month(01), 1+rand.Intn(10), 23, 12, 50, 789456478, time.UTC))
		}
		sym, err := model.NewDataTypeListWithRaw(model.DtInt, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListWithRaw(model.DtNanoTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := dbconnPool.RunScript("pt= loadTable(\"dfs://demohash\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_hash_range_date(t *testing.T) {
	Convey("TestDBConnectionPool_hash_range_date", t, func() {
		_, err := dbconnPool.RunScript("\n" +
			"t = table(nanotimestamp(1..10) as datev, symbol(string(1..10)) as sym)\n" +
			"db2=database(\"\",HASH,[SYMBOL,5])\n" +
			"db1=database(\"\",RANGE,date(1970.01.01)+0..100)\n" +
			"if(existsDatabase(\"dfs://demohash\")){\n" +
			"\tdropDatabase(\"dfs://demohash\")\n" +
			"}\n" +
			"db =database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
			"pt = db.createPartitionedTable(t,`pt,`sym`datev)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     setup.Address,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://demohash",
			TableName:    "pt",
			PartitionCol: "sym",
		}
		appender, err := api.NewPartitionedTableAppender(appenderOpt)
		So(err, ShouldBeNil)
		var symarr []string
		var datetimearr []time.Time
		rand.Seed(time.Now().Unix())
		for i := 0; i < 10000; i++ {
			symarr = append(symarr, strconv.Itoa(rand.Intn(10)))
			rand.Seed(time.Now().Unix())
			datetimearr = append(datetimearr, time.Date(1970, time.Month(01), 1+rand.Intn(10), 23, 12, 50, 789456478, time.UTC))
		}
		sym, err := model.NewDataTypeListWithRaw(model.DtSymbol, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListWithRaw(model.DtNanoTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := dbconnPool.RunScript("pt= loadTable(\"dfs://demohash\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_hash_range_datetime(t *testing.T) {
	Convey("TestDBConnectionPool_hash_range_datetime", t, func() {
		_, err := dbconnPool.RunScript("\n" +
			"t = table(datetime(1..10) as datev, symbol(string(1..10)) as sym)\n" +
			"db2=database(\"\",HASH,[SYMBOL,5])\n" +
			"db1=database(\"\",RANGE,datetime(1970.01.01T01:01:01)+0..10000*2)\n" +
			"if(existsDatabase(\"dfs://demohash\")){\n" +
			"\tdropDatabase(\"dfs://demohash\")\n" +
			"}\n" +
			"db =database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
			"pt = db.createPartitionedTable(t,`pt,`sym`datev)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     setup.Address,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://demohash",
			TableName:    "pt",
			PartitionCol: "sym",
		}
		appender, err := api.NewPartitionedTableAppender(appenderOpt)
		So(err, ShouldBeNil)
		var symarr []string
		var datetimearr []time.Time
		rand.Seed(time.Now().Unix())
		for i := 0; i < 10000; i++ {
			symarr = append(symarr, strconv.Itoa(rand.Intn(10)))
			rand.Seed(time.Now().Unix())
			datetimearr = append(datetimearr, time.Date(1970, time.Month(01), 01, 01, 01, 01+rand.Intn(10), 000, time.UTC))
		}
		sym, err := model.NewDataTypeListWithRaw(model.DtSymbol, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListWithRaw(model.DtDatetime, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := dbconnPool.RunScript("pt= loadTable(\"dfs://demohash\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_hash_value_symbol(t *testing.T) {
	Convey("TestDBConnectionPool_hash_value_symbol", t, func() {
		_, err := dbconnPool.RunScript("\n" +
			"t = table(datetime(1..10) as datev, symbol(string(1..10)) as sym)\n" +
			"db1=database(\"\",HASH,[DATETIME,10])\n" +
			"db2=database(\"\",VALUE,string(1..10))\n" +
			"if(existsDatabase(\"dfs://demohash\")){\n" +
			"\tdropDatabase(\"dfs://demohash\")\n" +
			"}\n" +
			"db =database(\"dfs://demohash\",COMPO,[db1,db2])\n" +
			"pt = db.createPartitionedTable(t,`pt,`datev`sym)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     setup.Address,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://demohash",
			TableName:    "pt",
			PartitionCol: "sym",
		}
		appender, err := api.NewPartitionedTableAppender(appenderOpt)
		So(err, ShouldBeNil)
		var symarr []string
		var datetimearr []time.Time
		rand.Seed(time.Now().Unix())
		for i := 0; i < 10000; i++ {
			symarr = append(symarr, strconv.Itoa(rand.Intn(10)))
			rand.Seed(time.Now().Unix())
			datetimearr = append(datetimearr, time.Date(2020, time.Month(02), 02, 01, 01, 01+rand.Intn(10), 000, time.UTC))
		}
		sym, err := model.NewDataTypeListWithRaw(model.DtSymbol, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListWithRaw(model.DtDatetime, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := dbconnPool.RunScript("pt= loadTable(\"dfs://demohash\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_value_value_date(t *testing.T) {
	Convey("TestDBConnectionPool_value_value_date", t, func() {
		_, err := dbconnPool.RunScript("\n" +
			"t = table(timestamp(1..10) as datev,string(1..10) as sym)\n" +
			"db2=database(\"\",VALUE,string(1..10))\n" +
			"db1=database(\"\",VALUE,date(2020.02.02)+0..100)\n" +
			"if(existsDatabase(\"dfs://demohash\")){\n" +
			"\tdropDatabase(\"dfs://demohash\")\n" +
			"}\n" +
			"db =database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
			"pt = db.createPartitionedTable(t,`pt,`sym`datev)\n")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     setup.Address,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://demohash",
			TableName:    "pt",
			PartitionCol: "sym",
		}
		appender, err := api.NewPartitionedTableAppender(appenderOpt)
		So(err, ShouldBeNil)
		var symarr []string
		var datetimearr []time.Time
		rand.Seed(time.Now().Unix())
		for i := 0; i < 10000; i++ {
			symarr = append(symarr, strconv.Itoa(rand.Intn(10)))
			rand.Seed(time.Now().Unix())
			datetimearr = append(datetimearr, time.Date(2020, time.Month(02), 02, 01, 01, 01+rand.Intn(10), 000, time.UTC))
		}
		sym, err := model.NewDataTypeListWithRaw(model.DtSymbol, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListWithRaw(model.DtTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := dbconnPool.RunScript("pt= loadTable(\"dfs://demohash\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_value_value_month(t *testing.T) {
	Convey("TestDBConnectionPool_value_value_month", t, func() {
		_, err := dbconnPool.RunScript("\n" +
			"t = table(timestamp(1..10) as datev,string(1..10) as sym)\n" +
			"db2=database(\"\",VALUE,string(1..10))\n" +
			"db1=database(\"\",VALUE,month(2020.02M)+0..100)\n" +
			"if(existsDatabase(\"dfs://demohash\")){\n" +
			"\tdropDatabase(\"dfs://demohash\")\n" +
			"}\n" +
			"db =database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
			"pt = db.createPartitionedTable(t,`pt,`sym`datev)\n")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     setup.Address,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://demohash",
			TableName:    "pt",
			PartitionCol: "sym",
		}
		appender, err := api.NewPartitionedTableAppender(appenderOpt)
		So(err, ShouldBeNil)
		var symarr []string
		var datetimearr []time.Time
		rand.Seed(time.Now().Unix())
		for i := 0; i < 10000; i++ {
			symarr = append(symarr, strconv.Itoa(rand.Intn(10)))
			rand.Seed(time.Now().Unix())
			datetimearr = append(datetimearr, time.Date(2020, time.Month(02), 02, 01, 01, 01+rand.Intn(10), 000, time.UTC))
		}
		sym, err := model.NewDataTypeListWithRaw(model.DtSymbol, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListWithRaw(model.DtTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := dbconnPool.RunScript("pt= loadTable(\"dfs://demohash\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_range_value_int(t *testing.T) {
	Convey("TestDBConnectionPool_range_value_int", t, func() {
		_, err := dbconnPool.RunScript("\n" +
			"t = table(timestamp(1..10) as datev,int(1..10) as sym)\n" +
			"db1=database(\"\",VALUE,date(now())+0..100)\n" +
			"db2=database(\"\",RANGE,int(0..11))\n" +
			"if(existsDatabase(\"dfs://demohash\")){\n" +
			"\tdropDatabase(\"dfs://demohash\")\n" +
			"}\n" +
			"db =database(\"dfs://demohash\",COMPO,[db1,db2])\n" +
			"pt = db.createPartitionedTable(t,`pt,`datev`sym)\n")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     setup.Address,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://demohash",
			TableName:    "pt",
			PartitionCol: "sym",
		}
		appender, err := api.NewPartitionedTableAppender(appenderOpt)
		So(err, ShouldBeNil)
		var symarr []int32
		var datetimearr []time.Time
		rand.Seed(time.Now().Unix())
		for i := 0; i < 10000; i++ {
			symarr = append(symarr, int32(rand.Intn(10)))
			rand.Seed(time.Now().Unix())
			datetimearr = append(datetimearr, time.Date(2020, time.Month(02), 02, 01, 01, 01+rand.Intn(10), 000, time.UTC))
		}
		sym, err := model.NewDataTypeListWithRaw(model.DtInt, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListWithRaw(model.DtTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := dbconnPool.RunScript("pt= loadTable(\"dfs://demohash\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_loadBalance_false(t *testing.T) {
	Convey("TestDBConnectionPool_loadBalance_false", t, func() {
		_, err := dbconnPool.RunScript("\n" +
			"t = table(timestamp(1..10) as datev,int(1..10) as sym)\n" +
			"db1=database(\"\",VALUE,date(now())+0..100)\n" +
			"db2=database(\"\",RANGE,int(0..11))\n" +
			"if(existsDatabase(\"dfs://demohash\")){\n" +
			"\tdropDatabase(\"dfs://demohash\")\n" +
			"}\n" +
			"db =database(\"dfs://demohash\",COMPO,[db1,db2])\n" +
			"pt = db.createPartitionedTable(t,`pt,`datev`sym)\n")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     setup.Address,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: false,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://demohash",
			TableName:    "pt",
			PartitionCol: "sym",
		}
		appender, err := api.NewPartitionedTableAppender(appenderOpt)
		So(err, ShouldBeNil)
		var symarr []int32
		var datetimearr []time.Time
		rand.Seed(time.Now().Unix())
		for i := 0; i < 10000; i++ {
			symarr = append(symarr, int32(rand.Intn(10)))
			rand.Seed(time.Now().Unix())
			datetimearr = append(datetimearr, time.Date(2020, time.Month(02), 02, 01, 01, 01+rand.Intn(10), 000, time.UTC))
		}
		sym, err := model.NewDataTypeListWithRaw(model.DtInt, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListWithRaw(model.DtTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := dbconnPool.RunScript("pt= loadTable(\"dfs://demohash\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestPartitionedTableAppender(t *testing.T) {
	Convey("Test_function_PartitionedTableAppender_prepare", t, func() {
		Convey("Test_function_PartitionedTableAppender_range_int", func() {
			_, err := dbconnPool.RunScript(`
        dbPath = "dfs://PTA_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100, ["sym", "id", "datev", "price"],[SYMBOL, INT, DATE, DOUBLE])
        db=database(dbPath, RANGE, [0, 11, 21, 31])
        pt = db.createPartitionedTable(t, "pt", "id")
        `)
			So(err, ShouldBeNil)
			pool := CreateDBConnectionPool(10, false)
			appenderOpt := &api.PartitionedTableAppenderOption{
				Pool:         pool,
				DBPath:       "dfs://PTA_test",
				TableName:    "pt",
				PartitionCol: "id",
			}
			appender, err := api.NewPartitionedTableAppender(appenderOpt)
			So(err, ShouldBeNil)
			sym, err := model.NewDataTypeListWithRaw(model.DtString, []string{"AAPL", "BLS", "DBKS", "NDLN", "DBKS"})
			So(err, ShouldBeNil)
			id, err := model.NewDataTypeListWithRaw(model.DtInt, []int32{2, 10, 12, 22, 23})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListWithRaw(model.DtDate, []time.Time{time.Date(1970, time.Month(1), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(3), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(10), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(5), 1, 1, 1, 0, 0, time.UTC)})
			So(err, ShouldBeNil)
			price, err := model.NewDataTypeListWithRaw(model.DtDouble, []float64{21.2, 4.4, 5.5, 2.3, 6.6})
			So(err, ShouldBeNil)
			newtable := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			num, err := appender.Append(newtable)
			So(err, ShouldBeNil)
			So(num, ShouldEqual, 5)
			re, err := dbconnPool.RunScript("select * from loadTable('dfs://PTA_test', 'pt')")
			So(err, ShouldBeNil)
			resultTable := re.(*model.Table)
			resultSym := resultTable.GetColumnByName("sym").Data.Value()
			tmp := []string{"AAPL", "BLS", "DBKS", "NDLN", "DBKS"}
			for i := 0; i < resultTable.Rows(); i++ {
				So(resultSym[i], ShouldEqual, tmp[i])
			}
			resultID := resultTable.GetColumnByName("id")
			So(resultID, ShouldResemble, model.NewVector(id))
			resultDatev := resultTable.GetColumnByName("datev")
			So(resultDatev, ShouldResemble, model.NewVector(datev))
			resultPrice := resultTable.GetColumnByName("price")
			So(resultPrice, ShouldResemble, model.NewVector(price))
			err = pool.Close()
			So(err, ShouldBeNil)
		})
		Convey("Test_function_PartitionedTableAppender_value_symbol", func() {
			_, err := dbconnPool.RunScript(`
		    dbPath = "dfs://PTA_test"
		    if(existsDatabase(dbPath))
		        dropDatabase(dbPath)
		    t = table(100:100, ["sym", "id", "datev", "price"],[SYMBOL, INT, DATE, DOUBLE])
		    db=database(dbPath, VALUE, symbol("A"+string(1..6)))
		    pt = db.createPartitionedTable(t, "pt", "sym")
		    `)
			So(err, ShouldBeNil)
			pool := CreateDBConnectionPool(10, false)
			appenderOpt := &api.PartitionedTableAppenderOption{
				Pool:         pool,
				DBPath:       "dfs://PTA_test",
				TableName:    "pt",
				PartitionCol: "sym",
			}
			appender, err := api.NewPartitionedTableAppender(appenderOpt)
			So(err, ShouldBeNil)
			sym, err := model.NewDataTypeListWithRaw(model.DtString, []string{"A1", "A2", "A3", "A4", "A5"})
			So(err, ShouldBeNil)
			id, err := model.NewDataTypeListWithRaw(model.DtInt, []int32{2, 7, 12, 22, 24})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListWithRaw(model.DtDate, []time.Time{time.Date(1970, time.Month(1), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(3), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(10), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(5), 1, 1, 1, 0, 0, time.UTC)})
			So(err, ShouldBeNil)
			price, err := model.NewDataTypeListWithRaw(model.DtDouble, []float64{21.2, 4.4, 5.5, 2.3, 6.6})
			So(err, ShouldBeNil)
			newtable := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			num, err := appender.Append(newtable)
			So(err, ShouldBeNil)
			So(num, ShouldEqual, 5)
			re, err := dbconnPool.RunScript("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, datev, price")
			So(err, ShouldBeNil)
			resultTable := re.(*model.Table)
			resultSym := resultTable.GetColumnByName("sym").Data.Value()
			tmp := []string{"A1", "A2", "A3", "A4", "A5"}
			for i := 0; i < resultTable.Rows(); i++ {
				So(resultSym[i], ShouldEqual, tmp[i])
			}
			resultID := resultTable.GetColumnByName("id")
			So(resultID, ShouldResemble, model.NewVector(id))
			resultDatev := resultTable.GetColumnByName("datev")
			So(resultDatev, ShouldResemble, model.NewVector(datev))
			resultPrice := resultTable.GetColumnByName("price")
			So(resultPrice, ShouldResemble, model.NewVector(price))
			err = pool.Close()
			So(err, ShouldBeNil)
		})
		Convey("Test_function_PartitionedTableAppender_hash_symbol", func() {
			_, err := dbconnPool.RunScript(`
		    dbPath = "dfs://PTA_test"
		    if(existsDatabase(dbPath))
		        dropDatabase(dbPath)
		    t = table(100:100, ["sym", "id", "datev", "price"],[SYMBOL, INT, DATE, DOUBLE])
		    db=database(dbPath, HASH, [SYMBOL, 5])
		    pt = db.createPartitionedTable(t, "pt", "sym")
		    `)
			So(err, ShouldBeNil)
			pool := CreateDBConnectionPool(10, false)
			appenderOpt := &api.PartitionedTableAppenderOption{
				Pool:         pool,
				DBPath:       "dfs://PTA_test",
				TableName:    "pt",
				PartitionCol: "sym",
			}
			appender, err := api.NewPartitionedTableAppender(appenderOpt)
			So(err, ShouldBeNil)
			sym, err := model.NewDataTypeListWithRaw(model.DtString, []string{"A1", "A2", "A3", "A4", "A5"})
			So(err, ShouldBeNil)
			id, err := model.NewDataTypeListWithRaw(model.DtInt, []int32{2, 7, 12, 22, 24})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListWithRaw(model.DtDate, []time.Time{time.Date(1970, time.Month(1), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(3), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(10), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(5), 1, 1, 1, 0, 0, time.UTC)})
			So(err, ShouldBeNil)
			price, err := model.NewDataTypeListWithRaw(model.DtDouble, []float64{21.2, 4.4, 5.5, 2.3, 6.6})
			So(err, ShouldBeNil)
			newtable := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			num, err := appender.Append(newtable)
			So(err, ShouldBeNil)
			So(num, ShouldEqual, 5)
			re, err := dbconnPool.RunScript("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, datev, price")
			So(err, ShouldBeNil)
			resultTable := re.(*model.Table)
			resultSym := resultTable.GetColumnByName("sym").Data.Value()
			tmp := []string{"A1", "A2", "A3", "A4", "A5"}
			for i := 0; i < resultTable.Rows(); i++ {
				So(resultSym[i], ShouldEqual, tmp[i])
			}
			resultID := resultTable.GetColumnByName("id")
			So(resultID, ShouldResemble, model.NewVector(id))
			resultDatev := resultTable.GetColumnByName("datev")
			So(resultDatev, ShouldResemble, model.NewVector(datev))
			resultPrice := resultTable.GetColumnByName("price")
			So(resultPrice, ShouldResemble, model.NewVector(price))
			err = pool.Close()
			So(err, ShouldBeNil)
		})
		Convey("Test_function_PartitionedTableAppender_list_symbol", func() {
			_, err := dbconnPool.RunScript(`
		    dbPath = "dfs://PTA_test"
		    if(existsDatabase(dbPath))
		        dropDatabase(dbPath)
		    t = table(100:100, ["sym", "id", "datev", "price"],[SYMBOL, INT, DATE, DOUBLE])
		    db=database(dbPath, LIST, [["A1", "A2"], ["A3", "A4", "A5"]])
		    pt = db.createPartitionedTable(t, "pt", "sym")
		    `)
			So(err, ShouldBeNil)
			pool := CreateDBConnectionPool(10, false)
			appenderOpt := &api.PartitionedTableAppenderOption{
				Pool:         pool,
				DBPath:       "dfs://PTA_test",
				TableName:    "pt",
				PartitionCol: "sym",
			}
			appender, err := api.NewPartitionedTableAppender(appenderOpt)
			So(err, ShouldBeNil)
			sym, err := model.NewDataTypeListWithRaw(model.DtString, []string{"A1", "A2", "A3", "A4", "A5"})
			So(err, ShouldBeNil)
			id, err := model.NewDataTypeListWithRaw(model.DtInt, []int32{2, 7, 12, 22, 24})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListWithRaw(model.DtDate, []time.Time{time.Date(1970, time.Month(1), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(3), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(10), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(5), 1, 1, 1, 0, 0, time.UTC)})
			So(err, ShouldBeNil)
			price, err := model.NewDataTypeListWithRaw(model.DtDouble, []float64{21.2, 4.4, 5.5, 2.3, 6.6})
			So(err, ShouldBeNil)
			newtable := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			num, err := appender.Append(newtable)
			So(err, ShouldBeNil)
			So(num, ShouldEqual, 5)
			re, err := dbconnPool.RunScript("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, datev, price")
			So(err, ShouldBeNil)
			resultTable := re.(*model.Table)
			resultSym := resultTable.GetColumnByName("sym").Data.Value()
			tmp := []string{"A1", "A2", "A3", "A4", "A5"}
			for i := 0; i < resultTable.Rows(); i++ {
				So(resultSym[i], ShouldEqual, tmp[i])
			}
			resultID := resultTable.GetColumnByName("id")
			So(resultID, ShouldResemble, model.NewVector(id))
			resultDatev := resultTable.GetColumnByName("datev")
			So(resultDatev, ShouldResemble, model.NewVector(datev))
			resultPrice := resultTable.GetColumnByName("price")
			So(resultPrice, ShouldResemble, model.NewVector(price))
			err = pool.Close()
			So(err, ShouldBeNil)
		})
		Convey("Test_function_PartitionedTableAppender_compo_value_list_symbol", func() {
			_, err := dbconnPool.RunScript(`
				dbPath = "dfs://PTA_test"
				if(existsDatabase(dbPath)){dropDatabase(dbPath)}
				t=table(100:100, ["sym", "id", "datev", "price"], [SYMBOL, INT, DATE, DOUBLE])
				db1=database(, VALUE, 1969.12.30..1970.01.03)
				db=database(dbPath, LIST, [["A1", "A2"], ["A3", "A4", "A5"]])
				pt=db.createPartitionedTable(t, "pt", "sym")
		    `)
			So(err, ShouldBeNil)
			pool := CreateDBConnectionPool(10, false)
			appenderOpt := &api.PartitionedTableAppenderOption{
				Pool:         pool,
				DBPath:       "dfs://PTA_test",
				TableName:    "pt",
				PartitionCol: "sym",
			}
			appender, err := api.NewPartitionedTableAppender(appenderOpt)
			So(err, ShouldBeNil)
			sym, err := model.NewDataTypeListWithRaw(model.DtString, []string{"A1", "A2", "A3", "A4", "A5"})
			So(err, ShouldBeNil)
			id, err := model.NewDataTypeListWithRaw(model.DtInt, []int32{2, 7, 12, 22, 24})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListWithRaw(model.DtDate, []time.Time{time.Date(1970, time.Month(1), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(3), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(10), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(5), 1, 1, 1, 0, 0, time.UTC)})
			So(err, ShouldBeNil)
			price, err := model.NewDataTypeListWithRaw(model.DtDouble, []float64{21.2, 4.4, 5.5, 2.3, 6.6})
			So(err, ShouldBeNil)
			newtable := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			num, err := appender.Append(newtable)
			// fmt.Println(newtable)
			So(err, ShouldBeNil)
			So(num, ShouldEqual, 5)
			re, err := dbconnPool.RunScript("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, datev, price")
			So(err, ShouldBeNil)
			resultTable := re.(*model.Table)
			resultSym := resultTable.GetColumnByName("sym").Data.Value()
			tmp := []string{"A1", "A2", "A3", "A4", "A5"}
			for i := 0; i < resultTable.Rows(); i++ {
				So(resultSym[i], ShouldEqual, tmp[i])
			}
			resultID := resultTable.GetColumnByName("id")
			So(resultID, ShouldResemble, model.NewVector(id))
			resultDatev := resultTable.GetColumnByName("datev")
			So(resultDatev, ShouldResemble, model.NewVector(datev))
			resultPrice := resultTable.GetColumnByName("price")
			So(resultPrice, ShouldResemble, model.NewVector(price))
			err = pool.Close()
			So(err, ShouldBeNil)
		})
	})
}

func TestDBConnectionPool_task(t *testing.T) {
	Convey("TestDBConnectionPool_task_equal_PoolSize", t, func() {
		_, err := dbconnPool.RunScript("db_path = \"dfs://test_DBConnectionPool\";\n" +
			"if(existsDatabase(db_path)){\n" +
			"        dropDatabase(db_path)\n" +
			"}\n" +
			"db = database(db_path, VALUE, 1..100);\n" +
			"t = table(10:0,`id`sym`price`nodePort,[INT,SYMBOL,DOUBLE,INT])\n" +
			"pt1 = db.createPartitionedTable(t,`pt1,`id)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     setup.Address,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    100,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		re := pool.GetPoolSize()
		So(re, ShouldEqual, 100)
		taskList := []*api.Task{}
		for i := 0; i < 100; i++ {
			task := &api.Task{
				Script: "t = table(int(take(" + strconv.Itoa(i) + ",100)) as id,rand(`a`b`c`d,100) as sym,int(rand(100,100)) as price,take(getNodePort(),100) as node);" +
					"pt = loadTable(\"dfs://test_DBConnectionPool\",`pt1);" +
					"pt.append!(t)",
			}
			taskList = append(taskList, task)
		}
		err = pool.Execute(taskList)
		So(err, ShouldBeNil)
		resultData, err := dbconnPool.RunScript("int(exec count(*) from loadTable(\"dfs://test_DBConnectionPool\",`pt1))")
		So(err, ShouldBeNil)
		resultCount := resultData.(*model.Scalar)
		So(resultCount.Value(), ShouldEqual, 10000)
		reNodesPort, err := dbconnPool.RunScript("exec nodePort from loadTable(\"dfs://test_DBConnectionPool\",`pt1) group by nodePort order by nodePort")
		So(err, ShouldBeNil)
		exNodesPort, err := dbconnPool.RunScript("exec value from pnodeRun(getNodePort) order by value")
		So(err, ShouldBeNil)
		So(reNodesPort.String(), ShouldEqual, exNodesPort.String())
		closed := pool.IsClosed()
		So(closed, ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		closed = pool.IsClosed()
		So(closed, ShouldBeTrue)
	})
	Convey("TestDBConnectionPool_task_large_than_PoolSize", t, func() {
		_, err := dbconnPool.RunScript("db_path = \"dfs://test_DBConnectionPool\";\n" +
			"if(existsDatabase(db_path)){\n" +
			"        dropDatabase(db_path)\n" +
			"}\n" +
			"db = database(db_path, VALUE, 1..100);\n" +
			"t = table(10:0,`id`sym`price`nodePort,[INT,SYMBOL,DOUBLE,INT])\n" +
			"pt1 = db.createPartitionedTable(t,`pt1,`id)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     setup.Address,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    10,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		re := pool.GetPoolSize()
		So(re, ShouldEqual, 10)
		taskList := []*api.Task{}
		for i := 0; i < 100; i++ {
			task := &api.Task{
				Script: "t = table(int(take(" + strconv.Itoa(i) + ",100)) as id,rand(`a`b`c`d,100) as sym,int(rand(100,100)) as price,take(getNodePort(),100) as node);" +
					"pt = loadTable(\"dfs://test_DBConnectionPool\",`pt1);" +
					"pt.append!(t)",
			}
			taskList = append(taskList, task)
		}
		err = pool.Execute(taskList)
		So(err, ShouldBeNil)
		resultData, err := dbconnPool.RunScript("int(exec count(*) from loadTable(\"dfs://test_DBConnectionPool\",`pt1))")
		So(err, ShouldBeNil)
		resultCount := resultData.(*model.Scalar)
		So(resultCount.Value(), ShouldEqual, 10000)
		reNodesPort, err := dbconnPool.RunScript("exec nodePort from loadTable(\"dfs://test_DBConnectionPool\",`pt1) group by nodePort order by nodePort")
		So(err, ShouldBeNil)
		exNodesPort, err := dbconnPool.RunScript("exec value from pnodeRun(getNodePort) order by value")
		So(err, ShouldBeNil)
		So(reNodesPort.String(), ShouldEqual, exNodesPort.String())
		closed := pool.IsClosed()
		So(closed, ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		closed = pool.IsClosed()
		So(closed, ShouldBeTrue)
	})
}

func TestTableAppender(t *testing.T) {
	Convey("Test_function_TableAppender_prepare", t, func() {
		Convey("Test_function_TableAppender_range_int", func() {
			_, err := dbconnPool.RunScript(`
        t = table(100:0, ["sym", "id", "datev", "price"],[SYMBOL, INT, DATE, DOUBLE])
        `)
			So(err, ShouldBeNil)
			appenderOpt := &api.TableAppenderOption{
				TableName: "t",
				Conn:      dbconnPool,
			}
			appender := api.NewTableAppender(appenderOpt)
			sym, err := model.NewDataTypeListWithRaw(model.DtString, []string{"AAPL", "BLS", "DBKS", "NDLN", "DBKS"})
			So(err, ShouldBeNil)
			id, err := model.NewDataTypeListWithRaw(model.DtInt, []int32{2, 10, 12, 22, 23})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListWithRaw(model.DtDate, []time.Time{time.Date(1970, time.Month(1), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(3), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(10), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(5), 1, 1, 1, 0, 0, time.UTC)})
			So(err, ShouldBeNil)
			price, err := model.NewDataTypeListWithRaw(model.DtDouble, []float64{21.2, 4.4, 5.5, 2.3, 6.6})
			So(err, ShouldBeNil)
			newtable := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			// fmt.Println(newtable)
			_, err = appender.Append(newtable)
			So(err, ShouldBeNil)
			re, err := dbconnPool.RunScript("t")
			So(err, ShouldBeNil)
			resultTable := re.(*model.Table)
			resultSym := resultTable.GetColumnByName("sym").Data.Value()
			tmp := []string{"AAPL", "BLS", "DBKS", "NDLN", "DBKS"}
			for i := 0; i < resultTable.Rows(); i++ {
				So(resultSym[i], ShouldEqual, tmp[i])
			}
			resultID := resultTable.GetColumnByName("id")
			So(resultID, ShouldResemble, model.NewVector(id))
			resultDatev := resultTable.GetColumnByName("datev")
			So(resultDatev, ShouldResemble, model.NewVector(datev))
			resultPrice := resultTable.GetColumnByName("price")
			So(resultPrice, ShouldResemble, model.NewVector(price))
			IsClose := appender.IsClosed()
			So(IsClose, ShouldBeFalse)
			err = appender.Close()
			So(err, ShouldBeNil)
			IsClose = appender.IsClosed()
			So(IsClose, ShouldBeTrue)
		})
		Convey("Test_function_TableAppender_disk", func() {
			dbconnPoolx, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
			So(err, ShouldBeNil)
			_, err = dbconnPoolx.RunScript(`
		    dbPath = "` + DiskDBPath + `"
		    if(exists(dbPath))
		        rmdir(dbPath, true)
		    t = table(100:100, ["sym", "id", "datev", "price"],[SYMBOL, INT, DATE, DOUBLE])
		    db=database(dbPath, RANGE, symbol("A"+string(1..7)))
		    pt = db.createPartitionedTable(t, "pt", "sym")
		    `)
			So(err, ShouldBeNil)
			appenderOpt := &api.TableAppenderOption{
				DBPath:    DiskDBPath,
				TableName: "pt",
				Conn:      dbconnPoolx,
			}
			appender := api.NewTableAppender(appenderOpt)
			So(err, ShouldBeNil)
			sym, err := model.NewDataTypeListWithRaw(model.DtString, []string{"A1", "A2", "A3", "A4", "A5"})
			So(err, ShouldBeNil)
			id, err := model.NewDataTypeListWithRaw(model.DtInt, []int32{2, 7, 12, 22, 24})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListWithRaw(model.DtDate, []time.Time{time.Date(1970, time.Month(1), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(3), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(10), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(5), 1, 1, 1, 0, 0, time.UTC)})
			So(err, ShouldBeNil)
			price, err := model.NewDataTypeListWithRaw(model.DtDouble, []float64{21.2, 4.4, 5.5, 2.3, 6.6})
			So(err, ShouldBeNil)
			newtable := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			_, err = appender.Append(newtable)
			So(err, ShouldBeNil)
			re, err := dbconnPoolx.RunScript("select * from loadTable(\"" + DiskDBPath + "\", 'pt') order by id, sym, datev, price")
			So(err, ShouldBeNil)
			resultTable := re.(*model.Table)
			resultSym := resultTable.GetColumnByName("sym").Data.Value()
			tmp := []string{"A1", "A2", "A3", "A4", "A5"}
			for i := 0; i < resultTable.Rows(); i++ {
				So(resultSym[i], ShouldEqual, tmp[i])
			}
			resultID := resultTable.GetColumnByName("id")
			So(resultID, ShouldResemble, model.NewVector(id))
			resultDatev := resultTable.GetColumnByName("datev")
			So(resultDatev, ShouldResemble, model.NewVector(datev))
			resultPrice := resultTable.GetColumnByName("price")
			So(resultPrice, ShouldResemble, model.NewVector(price))
			IsClose := appender.IsClosed()
			So(IsClose, ShouldBeFalse)
			err = appender.Close()
			So(err, ShouldBeNil)
			IsClose = appender.IsClosed()
			So(IsClose, ShouldBeTrue)
			dbconnPoolx.Close()
		})
		Convey("Test_function_TableAppender_dfsTable", func() {
			dbconnPoolx, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
			So(err, ShouldBeNil)
			_, err = dbconnPoolx.RunScript(`
		    dbPath = "` + DfsDBPath + `"
		    if(existsDatabase(dbPath))
		        dropDatabase(dbPath)
		    t = table(100:100, ["sym", "id", "datev", "price"],[SYMBOL, INT, DATE, DOUBLE])
		    db=database(dbPath, VALUE, symbol("A"+string(1..6)))
		    pt = db.createPartitionedTable(t, "pt", "sym")
		    `)
			So(err, ShouldBeNil)
			pool := CreateDBConnectionPool(10, false)
			appenderOpt := &api.TableAppenderOption{
				DBPath:    DfsDBPath,
				TableName: "pt",
				Conn:      dbconnPoolx,
			}
			appender := api.NewTableAppender(appenderOpt)
			So(err, ShouldBeNil)
			sym, err := model.NewDataTypeListWithRaw(model.DtString, []string{"A1", "A2", "A3", "A4", "A5"})
			So(err, ShouldBeNil)
			id, err := model.NewDataTypeListWithRaw(model.DtInt, []int32{2, 7, 12, 22, 24})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListWithRaw(model.DtDate, []time.Time{time.Date(1970, time.Month(1), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(3), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(10), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(5), 1, 1, 1, 0, 0, time.UTC)})
			So(err, ShouldBeNil)
			price, err := model.NewDataTypeListWithRaw(model.DtDouble, []float64{21.2, 4.4, 5.5, 2.3, 6.6})
			So(err, ShouldBeNil)
			newtable := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			_, err = appender.Append(newtable)
			So(err, ShouldBeNil)
			re, err := dbconnPoolx.RunScript("select * from loadTable('" + DfsDBPath + "', 'pt') order by id, sym, datev, price")
			So(err, ShouldBeNil)
			resultTable := re.(*model.Table)
			resultSym := resultTable.GetColumnByName("sym").Data.Value()
			tmp := []string{"A1", "A2", "A3", "A4", "A5"}
			for i := 0; i < resultTable.Rows(); i++ {
				So(resultSym[i], ShouldEqual, tmp[i])
			}
			resultID := resultTable.GetColumnByName("id")
			So(resultID, ShouldResemble, model.NewVector(id))
			resultDatev := resultTable.GetColumnByName("datev")
			So(resultDatev, ShouldResemble, model.NewVector(datev))
			resultPrice := resultTable.GetColumnByName("price")
			So(resultPrice, ShouldResemble, model.NewVector(price))
			err = pool.Close()
			So(err, ShouldBeNil)
			err = dbconnPoolx.Close()
			So(err, ShouldBeNil)
		})
	})
}

func TestDBconnPoolClose(t *testing.T) {
	dbconnPool.Close()
}
