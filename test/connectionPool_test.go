package test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

var host1 = getRandomClusterAddress()
var globalConn, _ = api.NewSimpleDolphinDBClient(context.TODO(), host1, setup.UserName, setup.Password)

func average(connections []int) float64 {
	sum := 0.0
	for _, conn := range connections {
		sum += float64(conn)
	}
	return sum / float64(len(connections))
}

func standardDeviation(connections []int, avg float64) float64 {
	sum := 0.0
	for _, conn := range connections {
		deviation := float64(conn) - avg
		sum += deviation * deviation
	}
	variance := sum / float64(len(connections))
	return math.Sqrt(variance)
}

func CheckConnectionPool(NewConnectionNum []interface{}) bool {
	var origin []int = make([]int, len(NewConnectionNum))
	for i := 0; i < len(NewConnectionNum); i++ {
		origin[i] = int(NewConnectionNum[i].(int32))
	}
	avg := average(origin)
	stddev := standardDeviation(origin, avg)
	threshold := 0.3 // 方差与平均值的偏移比例
	if stddev/avg < threshold {
		fmt.Println("Load balancing is effective.")
		return true
	}
	fmt.Println("Load balancing is not effective.")
	return false
}

func WaitConnectionPoolSuccess() bool {
	var res bool
	for i := 0; i < 10; i++ {
		NewConnectionNum := GetConnectionNum()
		fmt.Println(NewConnectionNum)
		res = CheckConnectionPool(NewConnectionNum)
		if res {
			break
		}
		time.Sleep(3 * time.Second)
	}
	return res
}

func GetConnectionNum() []interface{} {
	Table, _ := globalConn.RunScript("select connectionNum, name from rpc(getControllerAlias(), getClusterPerf) where mode = 0 or mode=4")
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
	Table, _ := globalConn.RunScript("select connectionNum, name from rpc(getControllerAlias(), getClusterPerf) where mode = 0 or mode=4")
	tmpTable := Table.(*model.Table)
	connectionNumList := tmpTable.GetColumnByName(tmpTable.GetColumnNames()[0])
	connectionNum := connectionNumList.Data.Value()
	fmt.Printf("\nNewConnection:%v\n", connectionNum)
	return CheckConnectionPool(connectionNum)
}

func TestDBConnectionPool_exception(t *testing.T) {
	Convey("Test_function_DBConnectionPool_exception_test", t, func() {
		Convey("Test_function_DBConnectionPool_wrong_address_exception \n", func() {
			opt := &api.PoolOption{
				Address:     "999.999.12.14",
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
				Address:     host1,
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
				Address:     host1,
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
				Address:     host1,
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
				Address:     host1,
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
				Address:     host1,
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
				Address:              host1,
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
			Address:     host1,
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
	SkipConvey("Test_function_DBConnectionPool_LoadBalance_true", t, func() {
		OriginConnectionNum := GetOriginConnNum()
		fmt.Printf("\norigin connection:%v\n", OriginConnectionNum)
		opt := &api.PoolOption{
			Address:     host1,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    8,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		re := pool.GetPoolSize()
		So(re, ShouldEqual, 8)
		IsSucess := WaitConnectionPoolSuccess()
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
	SkipConvey("Test_function_DBConnectionPool_SetLoadBalanceAddress", t, func() {
		time.Sleep(3 * time.Second)
		OriginConnectionNum := GetConnectionNum()
		fmt.Printf("\norigin connection:%v\n", OriginConnectionNum)
		opt := &api.PoolOption{
			Address:              host1,
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
		IsSucess := WaitConnectionPoolSuccess()
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
	t.Parallel()
	dbname := generateRandomString(8)
	Convey("TestDBConnectionPool_hash_hash_string", t, func() {
		_, err := globalConn.RunScript("t = table(timestamp(1..10) as datev,string(1..10) as sym)\n" +
			"db1=database(\"\",HASH,[DATETIME,10])\n" +
			"db2=database(\"\",HASH,[STRING,5])\n" +
			"if(existsDatabase(\"dfs://" + dbname + "\")){\n" +
			"\tdropDatabase(\"dfs://" + dbname + "\")}\n" +
			"db=database(\"dfs://" + dbname + "\",COMPO,[db2,db1])\n" +
			"pt=db.createPartitionedTable(t,`pt,`sym`datev)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     host1,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://" + dbname,
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
		sym, err := model.NewDataTypeListFromRawData(model.DtString, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListFromRawData(model.DtTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := globalConn.RunScript("pt= loadTable(\"dfs://" + dbname + "\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_value_hash_symbol(t *testing.T) {
	t.Parallel()
	dbname := generateRandomString(8)
	Convey("TestDBConnectionPool_value_hash_symbol", t, func() {
		_, err := globalConn.RunScript("t = table(timestamp(1..10) as datev,string(1..10) as sym)\n" +
			"db1=database(\"\",VALUE,date(2022.01.01)+0..100)\n" +
			"db2=database(\"\",HASH,[STRING,5])\n" +
			"if(existsDatabase(\"dfs://" + dbname + "\")){\n" +
			"\tdropDatabase(\"dfs://" + dbname + "\")}\n" +
			"db=database(\"dfs://" + dbname + "\",COMPO,[db2,db1])\n" +
			"pt=db.createPartitionedTable(t,`pt,`sym`datev)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     host1,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://" + dbname,
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
		sym, err := model.NewDataTypeListFromRawData(model.DtString, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListFromRawData(model.DtTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := globalConn.RunScript("pt= loadTable(\"dfs://" + dbname + "\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_hash_hash_int(t *testing.T) {
	t.Parallel()
	dbname := generateRandomString(8)
	Convey("TestDBConnectionPool_hash_hash_int", t, func() {
		_, err := globalConn.RunScript("t = table(timestamp(1..10) as datev,1..10 as sym)\n" +
			"db1=database(\"\",HASH,[DATETIME,10])\n" +
			"db2=database(\"\",HASH,[INT,5])\n" +
			"if(existsDatabase(\"dfs://" + dbname + "\")){\n" +
			"\tdropDatabase(\"dfs://" + dbname + "\")}\n" +
			"db=database(\"dfs://" + dbname + "\",COMPO,[db2,db1])\n" +
			"pt=db.createPartitionedTable(t,`pt,`sym`datev)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     host1,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://" + dbname,
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
		sym, err := model.NewDataTypeListFromRawData(model.DtInt, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListFromRawData(model.DtTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := globalConn.RunScript("pt= loadTable(\"dfs://" + dbname + "\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_value_hash_datetime(t *testing.T) {
	t.Parallel()
	dbname := generateRandomString(8)
	Convey("TestDBConnectionPool_value_hash_datetime", t, func() {
		_, err := globalConn.RunScript("\n" +
			"t = table(datetime(1..10) as datev,string(1..10) as sym)\n" +
			"db2=database(\"\",VALUE,string(0..10))\n" +
			"db1=database(\"\",HASH,[DATETIME,10])\n" +
			"if(existsDatabase(\"dfs://" + dbname + "\")){\n" +
			"\tdropDatabase(\"dfs://" + dbname + "\")\n" +
			"}\n" +
			"db=database(\"dfs://" + dbname + "\",COMPO,[db2,db1])\n" +
			"pt=db.createPartitionedTable(t,`pt,`sym`datev)\n")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     host1,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://" + dbname,
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
		sym, err := model.NewDataTypeListFromRawData(model.DtString, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListFromRawData(model.DtDatetime, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := globalConn.RunScript("pt= loadTable(\"dfs://" + dbname + "\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_range_hash_date(t *testing.T) {
	t.Parallel()
	dbname := generateRandomString(8)
	Convey("TestDBConnectionPool_range_hash_date", t, func() {
		_, err := globalConn.RunScript("t = table(date(1..10) as datev,symbol(string(1..10)) as sym)\n" +
			"db1=database(\"\",RANGE,date([0, 5, 11]))\n" +
			"db2=database(\"\",HASH,[SYMBOL,15])\n" +
			"if(existsDatabase(\"dfs://" + dbname + "\")){\n" +
			"\tdropDatabase(\"dfs://" + dbname + "\")\n" +
			"}\n" +
			"db=database(\"dfs://" + dbname + "\",COMPO,[db1,db2])\n" +
			"pt=db.createPartitionedTable(t,`pt,`datev`sym)\n")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     host1,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://" + dbname,
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
		sym, err := model.NewDataTypeListFromRawData(model.DtSymbol, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListFromRawData(model.DtDate, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := globalConn.RunScript("pt= loadTable(\"dfs://" + dbname + "\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_range_range_int(t *testing.T) {
	t.Parallel()
	dbname := generateRandomString(8)
	Convey("TestDBConnectionPool_range_range_int", t, func() {
		_, err := globalConn.RunScript("\n" +
			"t = table(nanotimestamp(1..10) as datev, 1..10 as sym)\n" +
			"db1=database(\"\",RANGE,date(1970.01.01)+0..100*5)\n" +
			"db2=database(\"\",RANGE,0 2 4 6 8 11)\n" +
			"if(existsDatabase(\"dfs://" + dbname + "\")){\n" +
			"\tdropDatabase(\"dfs://" + dbname + "\")\n" +
			"}\n" +
			"db =database(\"dfs://" + dbname + "\",COMPO,[db1,db2])\n" +
			"pt = db.createPartitionedTable(t,`pt,`datev`sym)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     host1,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://" + dbname,
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
		sym, err := model.NewDataTypeListFromRawData(model.DtInt, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListFromRawData(model.DtNanoTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := globalConn.RunScript("pt= loadTable(\"dfs://" + dbname + "\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_value_range_int(t *testing.T) {
	t.Parallel()
	dbname := generateRandomString(8)
	Convey("TestDBConnectionPool_value_range_int", t, func() {
		_, err := globalConn.RunScript("\n" +
			"t = table(timestamp(1..10) as datev,1..10 as sym)\n" +
			"db1=database(\"\",VALUE,date(1970.01.01)+0..10)\n" +
			"db2=database(\"\",RANGE,0 2 4 6 8 11)\n" +
			"if(existsDatabase(\"dfs://" + dbname + "\")){\n" +
			"\tdropDatabase(\"dfs://" + dbname + "\")\n" +
			"}\n" +
			"db =database(\"dfs://" + dbname + "\",COMPO,[db1,db2])\n" +
			"pt = db.createPartitionedTable(t,`pt,`datev`sym)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     host1,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://" + dbname,
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
		sym, err := model.NewDataTypeListFromRawData(model.DtInt, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListFromRawData(model.DtTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := globalConn.RunScript("pt= loadTable(\"dfs://" + dbname + "\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_range_range_month(t *testing.T) {
	t.Parallel()
	dbname := generateRandomString(8)
	Convey("TestDBConnectionPool_range_range_month", t, func() {
		_, err := globalConn.RunScript("\n" +
			"t = table(nanotimestamp(1..10) as datev,1..10 as sym)\n" +
			"db2=database(\"\",RANGE,0 2 4 6 8 11)\n" +
			"db1=database(\"\",RANGE,month(1970.01M)+0..100*5)\n" +
			"if(existsDatabase(\"dfs://" + dbname + "\")){\n" +
			"\tdropDatabase(\"dfs://" + dbname + "\")\n" +
			"}\n" +
			"db =database(\"dfs://" + dbname + "\",COMPO,[db2,db1])\n" +
			"pt = db.createPartitionedTable(t,`pt,`sym`datev)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     host1,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://" + dbname,
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
		sym, err := model.NewDataTypeListFromRawData(model.DtInt, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListFromRawData(model.DtNanoTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := globalConn.RunScript("pt= loadTable(\"dfs://" + dbname + "\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_hash_range_date(t *testing.T) {
	t.Parallel()
	dbname := generateRandomString(8)
	Convey("TestDBConnectionPool_hash_range_date", t, func() {
		_, err := globalConn.RunScript("\n" +
			"t = table(nanotimestamp(1..10) as datev, symbol(string(1..10)) as sym)\n" +
			"db2=database(\"\",HASH,[SYMBOL,5])\n" +
			"db1=database(\"\",RANGE,date(1970.01.01)+0..100)\n" +
			"if(existsDatabase(\"dfs://" + dbname + "\")){\n" +
			"\tdropDatabase(\"dfs://" + dbname + "\")\n" +
			"}\n" +
			"db =database(\"dfs://" + dbname + "\",COMPO,[db2,db1])\n" +
			"pt = db.createPartitionedTable(t,`pt,`sym`datev)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     host1,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://" + dbname,
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
		sym, err := model.NewDataTypeListFromRawData(model.DtSymbol, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListFromRawData(model.DtNanoTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := globalConn.RunScript("pt= loadTable(\"dfs://" + dbname + "\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_hash_range_datetime(t *testing.T) {
	t.Parallel()
	dbname := generateRandomString(8)
	Convey("TestDBConnectionPool_hash_range_datetime", t, func() {
		_, err := globalConn.RunScript("\n" +
			"t = table(datetime(1..10) as datev, symbol(string(1..10)) as sym)\n" +
			"db2=database(\"\",HASH,[SYMBOL,5])\n" +
			"db1=database(\"\",RANGE,datetime(1970.01.01T01:01:01)+0..10000*2)\n" +
			"if(existsDatabase(\"dfs://" + dbname + "\")){\n" +
			"\tdropDatabase(\"dfs://" + dbname + "\")\n" +
			"}\n" +
			"db =database(\"dfs://" + dbname + "\",COMPO,[db2,db1])\n" +
			"pt = db.createPartitionedTable(t,`pt,`sym`datev)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     host1,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://" + dbname,
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
		sym, err := model.NewDataTypeListFromRawData(model.DtSymbol, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListFromRawData(model.DtDatetime, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := globalConn.RunScript("pt= loadTable(\"dfs://" + dbname + "\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_hash_value_symbol(t *testing.T) {
	t.Parallel()
	dbname := generateRandomString(8)
	Convey("TestDBConnectionPool_hash_value_symbol", t, func() {
		_, err := globalConn.RunScript("\n" +
			"t = table(datetime(1..10) as datev, symbol(string(1..10)) as sym)\n" +
			"db1=database(\"\",HASH,[DATETIME,10])\n" +
			"db2=database(\"\",VALUE,string(1..10))\n" +
			"if(existsDatabase(\"dfs://" + dbname + "\")){\n" +
			"\tdropDatabase(\"dfs://" + dbname + "\")\n" +
			"}\n" +
			"db =database(\"dfs://" + dbname + "\",COMPO,[db1,db2])\n" +
			"pt = db.createPartitionedTable(t,`pt,`datev`sym)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     host1,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://" + dbname,
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
		sym, err := model.NewDataTypeListFromRawData(model.DtSymbol, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListFromRawData(model.DtDatetime, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := globalConn.RunScript("pt= loadTable(\"dfs://" + dbname + "\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_value_value_date(t *testing.T) {
	t.Parallel()
	dbname := generateRandomString(8)
	Convey("TestDBConnectionPool_value_value_date", t, func() {
		_, err := globalConn.RunScript("\n" +
			"t = table(timestamp(1..10) as datev,string(1..10) as sym)\n" +
			"db2=database(\"\",VALUE,string(1..10))\n" +
			"db1=database(\"\",VALUE,date(2020.02.02)+0..100)\n" +
			"if(existsDatabase(\"dfs://" + dbname + "\")){\n" +
			"\tdropDatabase(\"dfs://" + dbname + "\")\n" +
			"}\n" +
			"db =database(\"dfs://" + dbname + "\",COMPO,[db2,db1])\n" +
			"pt = db.createPartitionedTable(t,`pt,`sym`datev)\n")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     host1,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://" + dbname,
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
		sym, err := model.NewDataTypeListFromRawData(model.DtSymbol, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListFromRawData(model.DtTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := globalConn.RunScript("pt= loadTable(\"dfs://" + dbname + "\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_value_value_month(t *testing.T) {
	t.Parallel()
	dbname := generateRandomString(8)
	Convey("TestDBConnectionPool_value_value_month", t, func() {
		_, err := globalConn.RunScript("\n" +
			"t = table(timestamp(1..10) as datev,string(1..10) as sym)\n" +
			"db2=database(\"\",VALUE,string(1..10))\n" +
			"db1=database(\"\",VALUE,month(2020.02M)+0..100)\n" +
			"if(existsDatabase(\"dfs://" + dbname + "\")){\n" +
			"\tdropDatabase(\"dfs://" + dbname + "\")\n" +
			"}\n" +
			"db =database(\"dfs://" + dbname + "\",COMPO,[db2,db1])\n" +
			"pt = db.createPartitionedTable(t,`pt,`sym`datev)\n")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     host1,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://" + dbname,
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
		sym, err := model.NewDataTypeListFromRawData(model.DtSymbol, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListFromRawData(model.DtTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := globalConn.RunScript("pt= loadTable(\"dfs://" + dbname + "\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_range_value_int(t *testing.T) {
	t.Parallel()
	dbname := generateRandomString(8)
	Convey("TestDBConnectionPool_range_value_int", t, func() {
		_, err := globalConn.RunScript("\n" +
			"t = table(timestamp(1..10) as datev,int(1..10) as sym)\n" +
			"db1=database(\"\",VALUE,date(now())+0..100)\n" +
			"db2=database(\"\",RANGE,int(0..11))\n" +
			"if(existsDatabase(\"dfs://" + dbname + "\")){\n" +
			"\tdropDatabase(\"dfs://" + dbname + "\")\n" +
			"}\n" +
			"db =database(\"dfs://" + dbname + "\",COMPO,[db1,db2])\n" +
			"pt = db.createPartitionedTable(t,`pt,`datev`sym)\n")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     host1,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://" + dbname,
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
		sym, err := model.NewDataTypeListFromRawData(model.DtInt, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListFromRawData(model.DtTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := globalConn.RunScript("pt= loadTable(\"dfs://" + dbname + "\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_loadBalance_false(t *testing.T) {
	t.Parallel()
	dbname := generateRandomString(8)
	Convey("TestDBConnectionPool_loadBalance_false", t, func() {
		_, err := globalConn.RunScript("\n" +
			"t = table(timestamp(1..10) as datev,int(1..10) as sym)\n" +
			"db1=database(\"\",VALUE,date(now())+0..100)\n" +
			"db2=database(\"\",RANGE,int(0..11))\n" +
			"if(existsDatabase(\"dfs://" + dbname + "\")){\n" +
			"\tdropDatabase(\"dfs://" + dbname + "\")\n" +
			"}\n" +
			"db =database(\"dfs://" + dbname + "\",COMPO,[db1,db2])\n" +
			"pt = db.createPartitionedTable(t,`pt,`datev`sym)\n")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     host1,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    3,
			LoadBalance: false,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://" + dbname,
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
		sym, err := model.NewDataTypeListFromRawData(model.DtInt, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListFromRawData(model.DtTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		for i := 0; i < 100; i++ {
			num, err := appender.Append(newtable)
			AssertNil(err)
			AssertEqual(num, 10000)
		}
		re, err := globalConn.RunScript("pt= loadTable(\"dfs://" + dbname + "\",`pt)\n" +
			"exec count(*) from pt")
		So(err, ShouldBeNil)
		resultCount := re.(*model.Scalar).Value()
		So(resultCount, ShouldEqual, int64(1000000))
		So(pool.IsClosed(), ShouldBeFalse)
		globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestPartitionedTableAppender(t *testing.T) {
	t.Parallel()
	Convey("Test_function_PartitionedTableAppender_prepare", t, func() {
		Convey("Test_function_PartitionedTableAppender_range_int", func() {
			dbname := generateRandomString(8)
			_, err := globalConn.RunScript(`
			dbPath = "dfs://` + dbname + `"
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
				DBPath:       "dfs://" + dbname,
				TableName:    "pt",
				PartitionCol: "id",
			}
			appender, err := api.NewPartitionedTableAppender(appenderOpt)
			So(err, ShouldBeNil)
			sym, err := model.NewDataTypeListFromRawData(model.DtString, []string{"AAPL", "BLS", "DBKS", "NDLN", "DBKS"})
			So(err, ShouldBeNil)
			id, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{2, 10, 12, 22, 23})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{time.Date(1970, time.Month(1), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(3), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(10), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(5), 1, 1, 1, 0, 0, time.UTC)})
			So(err, ShouldBeNil)
			price, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{21.2, 4.4, 5.5, 2.3, 6.6})
			So(err, ShouldBeNil)
			newtable := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			num, err := appender.Append(newtable)
			So(err, ShouldBeNil)
			So(num, ShouldEqual, 5)
			re, err := globalConn.RunScript("select * from loadTable('dfs://" + dbname + "', 'pt')")
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
			dbname := generateRandomString(8)
			_, err := globalConn.RunScript(`
				dbPath = "dfs://` + dbname + `"
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
				DBPath:       "dfs://" + dbname,
				TableName:    "pt",
				PartitionCol: "sym",
			}
			appender, err := api.NewPartitionedTableAppender(appenderOpt)
			So(err, ShouldBeNil)
			sym, err := model.NewDataTypeListFromRawData(model.DtString, []string{"A1", "A2", "A3", "A4", "A5"})
			So(err, ShouldBeNil)
			id, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{2, 7, 12, 22, 24})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{time.Date(1970, time.Month(1), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(3), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(10), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(5), 1, 1, 1, 0, 0, time.UTC)})
			So(err, ShouldBeNil)
			price, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{21.2, 4.4, 5.5, 2.3, 6.6})
			So(err, ShouldBeNil)
			newtable := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			num, err := appender.Append(newtable)
			So(err, ShouldBeNil)
			So(num, ShouldEqual, 5)
			re, err := globalConn.RunScript("select * from loadTable('dfs://" + dbname + "', 'pt') order by id, sym, datev, price")
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
			dbname := generateRandomString(8)
			_, err := globalConn.RunScript(`
			dbPath = "dfs://` + dbname + `"
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
				DBPath:       "dfs://" + dbname,
				TableName:    "pt",
				PartitionCol: "sym",
			}
			appender, err := api.NewPartitionedTableAppender(appenderOpt)
			So(err, ShouldBeNil)
			sym, err := model.NewDataTypeListFromRawData(model.DtString, []string{"A1", "A2", "A3", "A4", "A5"})
			So(err, ShouldBeNil)
			id, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{2, 7, 12, 22, 24})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{time.Date(1970, time.Month(1), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(3), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(10), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(5), 1, 1, 1, 0, 0, time.UTC)})
			So(err, ShouldBeNil)
			price, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{21.2, 4.4, 5.5, 2.3, 6.6})
			So(err, ShouldBeNil)
			newtable := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			num, err := appender.Append(newtable)
			So(err, ShouldBeNil)
			So(num, ShouldEqual, 5)
			re, err := globalConn.RunScript("select * from loadTable('dfs://" + dbname + "', 'pt') order by id, sym, datev, price")
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
			dbname := generateRandomString(8)
			_, err := globalConn.RunScript(`
			dbPath = "dfs://` + dbname + `"
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
				DBPath:       "dfs://" + dbname,
				TableName:    "pt",
				PartitionCol: "sym",
			}
			appender, err := api.NewPartitionedTableAppender(appenderOpt)
			So(err, ShouldBeNil)
			sym, err := model.NewDataTypeListFromRawData(model.DtString, []string{"A1", "A2", "A3", "A4", "A5"})
			So(err, ShouldBeNil)
			id, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{2, 7, 12, 22, 24})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{time.Date(1970, time.Month(1), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(3), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(10), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(5), 1, 1, 1, 0, 0, time.UTC)})
			So(err, ShouldBeNil)
			price, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{21.2, 4.4, 5.5, 2.3, 6.6})
			So(err, ShouldBeNil)
			newtable := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			num, err := appender.Append(newtable)
			So(err, ShouldBeNil)
			So(num, ShouldEqual, 5)
			re, err := globalConn.RunScript("select * from loadTable('dfs://" + dbname + "', 'pt') order by id, sym, datev, price")
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
			dbname := generateRandomString(8)
			_, err := globalConn.RunScript(`
				dbPath = "dfs://` + dbname + `"
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
				DBPath:       "dfs://" + dbname,
				TableName:    "pt",
				PartitionCol: "sym",
			}
			appender, err := api.NewPartitionedTableAppender(appenderOpt)
			So(err, ShouldBeNil)
			sym, err := model.NewDataTypeListFromRawData(model.DtString, []string{"A1", "A2", "A3", "A4", "A5"})
			So(err, ShouldBeNil)
			id, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{2, 7, 12, 22, 24})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{time.Date(1970, time.Month(1), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(3), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(10), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(5), 1, 1, 1, 0, 0, time.UTC)})
			So(err, ShouldBeNil)
			price, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{21.2, 4.4, 5.5, 2.3, 6.6})
			So(err, ShouldBeNil)
			newtable := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			num, err := appender.Append(newtable)
			// fmt.Println(newtable)
			So(err, ShouldBeNil)
			So(num, ShouldEqual, 5)
			re, err := globalConn.RunScript("select * from loadTable('dfs://" + dbname + "', 'pt') order by id, sym, datev, price")
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

		Convey("Test_function_PartitionedTableAppender_arraVector", func() {
			var dbpath = "dfs://test_av"
			var tbname = "pt"
			var rows = 100
			_, err := globalConn.RunScript(`
				row_num=` + strconv.Itoa(rows) + `;
				ind = [2,4,6,8,10];
				cbool= arrayVector(ind, bool(take(0 1 ,10)));cchar = arrayVector(ind, char(take(256 ,10)));cshort = arrayVector(ind, short(take(-10000..10000 ,10)));cint = arrayVector(ind, int(take(-10000..10000 ,10)));
				clong = arrayVector(ind, long(take(-10000..10000 ,10)));cdate = arrayVector(ind, date(take(10000 ,10)));cmonth = arrayVector(ind, month(take(23640..25000 ,10)));ctime = arrayVector(ind, time(take(10000 ,10)));
				cminute = arrayVector(ind, minute(take(100 ,10)));csecond = arrayVector(ind, second(take(100 ,10)));cdatetime = arrayVector(ind, datetime(take(10000 ,10)));ctimestamp = arrayVector(ind, timestamp(take(10000 ,10)));
				cnanotime = arrayVector(ind, nanotime(take(10000 ,10)));cnanotimestamp = arrayVector(ind, nanotimestamp(take(10000 ,10)));cdatehour = arrayVector(ind, datehour(take(10000 ,10)));
				cfloat = arrayVector(ind, float(rand(10000.0000,10)));cdouble = arrayVector(ind, rand(10000.0000,10));
				cdecimal32 = array(DECIMAL32(6)[], 0, 0).append!(decimal32([1..2, [], rand(100.000000, 2), rand(1..100, 2), take(00i, 2)], 6));
				cdecimal64 = array(DECIMAL64(16)[], 0, 0).append!(decimal64([1..2, [], rand(100.000000, 2), rand(1..100, 2), take(00i, 2)], 16));
				cdecimal128 = array(DECIMAL128(26)[], 0, 0).append!(decimal128([1..2, [], rand(100.000000, 2), rand(1..100, 2), take(00i, 2)], 26));
				cipaddr = arrayVector(ind, take(ipaddr(["192.168.1.13","192.168.1.14"]),10));
				cuuid = arrayVector(ind, take(uuid(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee88"]),10));
				cint128 = arrayVector(ind, take(int128(["e1671797c52e15f763380b45e841ec32","e1671797c52e15f763380b45e841ec33"]),10));

				for(i in 1..(row_num-5)){
					cbool.append!([bool(take(0 1 ,2))]);
					cchar.append!([char(rand(256 ,2))]);cshort.append!([short(rand(-10000..10000 ,2))]);cint.append!([int(rand(-10000..10000 ,2))]);
					clong.append!([long(rand(-10000..10000 ,2))]);cdate.append!([date(rand(10000 ,2))]);cmonth.append!([month(rand(23640..25000 ,2))]);
					ctime.append!([time(rand(10000 ,2))]);cminute.append!([minute(rand(100 ,2))]);csecond.append!([second(rand(100 ,2))]);
					cdatetime.append!([datetime(rand(10000 ,2))]);ctimestamp.append!([timestamp(rand(10000 ,2))]);
					cnanotime.append!([nanotime(rand(10000 ,2))]);cnanotimestamp.append!([nanotimestamp(rand(10000 ,2))]);
					cdatehour.append!([datehour(rand(10000 ,2))]);
					cfloat.append!([float(rand(10000.0000,2))]);cdouble.append!([rand(10000.0000, 2)]);
					cdecimal32.append!([decimal32('1.123123123123123123123123123''-5.789' ,6)]);
					cdecimal64.append!([decimal64('1.123123123123123123123123123''-5.789' ,16)]);
					cdecimal128.append!([decimal128('1.123123123123123123123123123''-5.789' ,26)]);
					cipaddr.append!([take(ipaddr(["192.168.1.13","192.168.1.14"]),2)]);
					cuuid.append!([take(uuid(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee88"]),2)]);
					cint128.append!([take(int128(["e1671797c52e15f763380b45e841ec32","e1671797c52e15f763380b45e841ec33"]),2)]);
				};

				go;
				date_index = date(0..(row_num-1));
				int_index = 0..(row_num-1);
				table1=table(date_index,int_index,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cdatehour,cfloat,cdouble,cuuid,cint128,cipaddr,cdecimal32,cdecimal64,cdecimal128);
				tableInsert(table1, date(row_num),row_num,[take(true false,2)],[take(char(NULL),2)],[take(short(NULL),2)],[take(int(NULL),2)],[take(long(NULL),2)],[take(date(NULL),2)],[take(month(NULL),2)],[take(time(NULL),2)],[take(minute(NULL),2)],[take(second(NULL),2)],[take(datetime(NULL),2)],[take(timestamp(NULL),2)],[take(nanotime(NULL),2)],[take(nanotimestamp(NULL),2)],[take(datehour(NULL),2)],[take(float(NULL),2)],[take(double(NULL),2)],[take(uuid(string(NULL)),2)],[take(int128(string(NULL)),2)],[take(ipaddr(string(NULL)),2)],[take(decimal32(NULL,6),2)],[take(decimal64(NULL,16),2)],[take(decimal128(NULL,26),2)]);
				share table1 as origin_tab;
				dbpath = "` + dbpath + `";
				tbname = "` + tbname + `";
				if(existsDatabase(dbpath)){dropDatabase(dbpath)};
				db = database(dbpath, HASH, [DATE, 2], engine="TSDB");
				db.createPartitionedTable(table1, tbname, 'date_index', , 'int_index''date_index');
		    `)
			So(err, ShouldBeNil)
			tab, err := globalConn.RunScript(`select * from origin_tab`)
			So(err, ShouldBeNil)
			// fmt.Println(tab)
			pool := CreateDBConnectionPool(2, false)
			appenderOpt := &api.PartitionedTableAppenderOption{
				Pool:         pool,
				DBPath:       dbpath,
				TableName:    tbname,
				PartitionCol: "date_index",
			}
			appender, err := api.NewPartitionedTableAppender(appenderOpt)

			So(err, ShouldBeNil)
			num, err := appender.Append(tab.(*model.Table))

			// fmt.Println(newtable)
			So(err, ShouldBeNil)
			So(num, ShouldEqual, rows+1)
			_, err = globalConn.RunScript(
				"res = select * from loadTable('" + dbpath + "', '" + tbname + "') order by date_index, int_index;" +
					"ex = select * from origin_tab order by date_index, int_index;" +
					"assert 1, each(eqObj, res.values(), ex.values())")
			So(err, ShouldBeNil)
			_, err = globalConn.RunScript(`undef('origin_tab', SHARED)`)
			So(err, ShouldBeNil)
		})
	})
}

func TestDBConnectionPool_task(t *testing.T) {
	t.Parallel()
	Convey("TestDBConnectionPool_task_equal_PoolSize", t, func() {
		dbname := generateRandomString(8)
		_, err := globalConn.RunScript("db_path = \"dfs://" + dbname + "\";\n" +
			"if(existsDatabase(db_path)){\n" +
			"        dropDatabase(db_path)\n" +
			"}\n" +
			"db = database(db_path, VALUE, 1..100);\n" +
			"t = table(10:0,`id`sym`price`nodePort,[INT,SYMBOL,DOUBLE,INT])\n" +
			"pt1 = db.createPartitionedTable(t,`pt1,`id)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     host1,
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
					"pt = loadTable(\"dfs://" + dbname + "\",`pt1);" +
					"pt.append!(t)",
			}
			taskList = append(taskList, task)
		}
		err = pool.Execute(taskList)
		So(err, ShouldBeNil)
		resultData, err := globalConn.RunScript("int(exec count(*) from loadTable(\"dfs://" + dbname + "\",`pt1))")
		So(err, ShouldBeNil)
		resultCount := resultData.(*model.Scalar)
		So(resultCount.Value(), ShouldEqual, 10000)
		reNodesPort, err := globalConn.RunScript("exec nodePort from loadTable(\"dfs://" + dbname + "\",`pt1) group by nodePort order by nodePort")
		So(err, ShouldBeNil)
		exNodesPort, err := globalConn.RunScript("exec value from pnodeRun(getNodePort) order by value")
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
		dbname := generateRandomString(8)
		_, err := globalConn.RunScript("db_path = \"dfs://" + dbname + "\";\n" +
			"if(existsDatabase(db_path)){\n" +
			"        dropDatabase(db_path)\n" +
			"}\n" +
			"db = database(db_path, VALUE, 1..100);\n" +
			"t = table(10:0,`id`sym`price`nodePort,[INT,SYMBOL,DOUBLE,INT])\n" +
			"pt1 = db.createPartitionedTable(t,`pt1,`id)")
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			Address:     host1,
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
					"pt = loadTable(\"dfs://" + dbname + "\",`pt1);" +
					"pt.append!(t)",
			}
			taskList = append(taskList, task)
		}
		err = pool.Execute(taskList)
		So(err, ShouldBeNil)
		resultData, err := globalConn.RunScript("int(exec count(*) from loadTable(\"dfs://" + dbname + "\",`pt1))")
		So(err, ShouldBeNil)
		resultCount := resultData.(*model.Scalar)
		So(resultCount.Value(), ShouldEqual, 10000)
		reNodesPort, err := globalConn.RunScript("exec nodePort from loadTable(\"dfs://" + dbname + "\",`pt1) group by nodePort order by nodePort")
		So(err, ShouldBeNil)
		exNodesPort, err := globalConn.RunScript("exec value from pnodeRun(getNodePort) order by value")
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
	t.Parallel()
	Convey("Test_function_TableAppender_prepare", t, func() {
		Convey("Test_function_TableAppender_range_int", func() {
			_, err := globalConn.RunScript(`
        t = table(100:0, ["sym", "id", "datev", "price"],[SYMBOL, INT, DATE, DOUBLE])
        `)
			So(err, ShouldBeNil)
			appenderOpt := &api.TableAppenderOption{
				TableName: "t",
				Conn:      globalConn,
			}
			appender := api.NewTableAppender(appenderOpt)
			sym, err := model.NewDataTypeListFromRawData(model.DtString, []string{"AAPL", "BLS", "DBKS", "NDLN", "DBKS"})
			So(err, ShouldBeNil)
			id, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{2, 10, 12, 22, 23})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{time.Date(1970, time.Month(1), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(3), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(10), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(5), 1, 1, 1, 0, 0, time.UTC)})
			So(err, ShouldBeNil)
			price, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{21.2, 4.4, 5.5, 2.3, 6.6})
			So(err, ShouldBeNil)
			newtable := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			// fmt.Println(newtable)
			_, err = appender.Append(newtable)
			So(err, ShouldBeNil)
			re, err := globalConn.RunScript("t")
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
			globalConnx, err := api.NewSimpleDolphinDBClient(context.TODO(), host1, setup.UserName, setup.Password)
			So(err, ShouldBeNil)
			_, err = globalConnx.RunScript(`
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
				Conn:      globalConnx,
			}
			appender := api.NewTableAppender(appenderOpt)
			So(err, ShouldBeNil)
			sym, err := model.NewDataTypeListFromRawData(model.DtString, []string{"A1", "A2", "A3", "A4", "A5"})
			So(err, ShouldBeNil)
			id, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{2, 7, 12, 22, 24})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{time.Date(1970, time.Month(1), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(3), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(10), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(5), 1, 1, 1, 0, 0, time.UTC)})
			So(err, ShouldBeNil)
			price, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{21.2, 4.4, 5.5, 2.3, 6.6})
			So(err, ShouldBeNil)
			newtable := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			_, err = appender.Append(newtable)
			So(err, ShouldBeNil)
			re, err := globalConnx.RunScript("select * from loadTable(\"" + DiskDBPath + "\", 'pt') order by id, sym, datev, price")
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
			globalConnx.Close()
		})
		Convey("Test_function_TableAppender_dfsTable", func() {
			globalConnx, err := api.NewSimpleDolphinDBClient(context.TODO(), host1, setup.UserName, setup.Password)
			So(err, ShouldBeNil)
			DfsDBPath := "dfs://" + generateRandomString(8)
			_, err = globalConnx.RunScript(`
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
				Conn:      globalConnx,
			}
			appender := api.NewTableAppender(appenderOpt)
			So(err, ShouldBeNil)
			sym, err := model.NewDataTypeListFromRawData(model.DtString, []string{"A1", "A2", "A3", "A4", "A5"})
			So(err, ShouldBeNil)
			id, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{2, 7, 12, 22, 24})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{time.Date(1970, time.Month(1), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(3), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(10), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(5), 1, 1, 1, 0, 0, time.UTC)})
			So(err, ShouldBeNil)
			price, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{21.2, 4.4, 5.5, 2.3, 6.6})
			So(err, ShouldBeNil)
			newtable := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			_, err = appender.Append(newtable)
			So(err, ShouldBeNil)
			re, err := globalConnx.RunScript("select * from loadTable('" + DfsDBPath + "', 'pt') order by id, sym, datev, price")
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
			err = globalConnx.Close()
			So(err, ShouldBeNil)
		})
	})
}

func TestConnnectionPoolHighAvailability(t *testing.T) {
	t.Parallel()
	SkipConvey("TestConnnectionPoolHighAvailability", t, func() {
		opt := &api.PoolOption{
			Address:                setup.Address4,
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               10,
			EnableHighAvailability: true,
			HighAvailabilitySites:  setup.HA_sites,
		}
		poolHA, err := api.NewDBConnectionPool(opt)
		AssertNil(err)
		poolCtl, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.CtlAdress, setup.UserName, setup.Password)
		AssertNil(err)
		getnametask := api.Task{Script: "getNodeAlias()"}
		tasks := []*api.Task{&getnametask}

		err = poolHA.Execute(tasks)
		AssertNil(err)
		origin_node := tasks[0].GetResult()
		fmt.Println("now", origin_node.(*model.Scalar).Value().(string), "is connected, try to stop it")
		poolCtl.RunScript("stopDataNode(`" + origin_node.(*model.Scalar).Value().(string) + ")")
		time.Sleep(2 * time.Second)
		fmt.Println("stop success, check if the origin connection click to another node")
		err = poolHA.Execute(tasks)
		AssertNil(err)
		So(tasks[0].GetResult().String(), ShouldNotEqual, origin_node.(*model.Scalar).Value().(string))
		fmt.Println("check passed, restart the origin node")
		_, err = poolCtl.RunScript(
			"nodes = exec name from getClusterPerf() where state!=1 and mode !=1;" +
				"startDataNode(nodes);")
		AssertNil(err)
		time.Sleep(2 * time.Second)
		poolCtl.Close()
		poolHA.Close()
	})
	// Convey("TestConnnectionHighAvailability exception", t, func() {
	// 	opt := &api.PoolOption{
	// 		Address:                setup.Address4,
	// 		UserID:                 setup.UserName,
	// 		Password:               setup.Password,
	// 		PoolSize:               10,
	// 		EnableHighAvailability: true,
	// 		// HighAvailabilitySites:  setup.HA_sites,
	// 	}
	// 	_, err := api.NewDBConnectionPool(opt)
	// 	So(err.Error(), ShouldContainSubstring, "connect to all sites failed")

	// 	opt = &api.PoolOption{
	// 		Address:  setup.Address4,
	// 		UserID:   setup.UserName,
	// 		Password: setup.Password,
	// 		PoolSize: 10,
	// 		// EnableHighAvailability: true,
	// 		HighAvailabilitySites: setup.HA_sites,
	// 	}
	// 	_, err = api.NewDBConnectionPool(opt)
	// 	So(err.Error(), ShouldContainSubstring, "connect to all sites failed")

	// 	opt = &api.PoolOption{
	// 		Address:                setup.Address4,
	// 		UserID:                 setup.UserName,
	// 		Password:               setup.Password,
	// 		PoolSize:               10,
	// 		EnableHighAvailability: false,
	// 		HighAvailabilitySites:  setup.HA_sites,
	// 	}
	// 	_, err = api.NewDBConnectionPool(opt)
	// 	So(err.Error(), ShouldContainSubstring, "connect to all sites failed")
	// })

}

func TestConnnectionPooltimeOut(t *testing.T) {
	t.Parallel()
	Convey("TestConnnectionPooltimeOut_timeoutOption", t, func() {
		opt := &api.PoolOption{
			Address:  setup.Address4,
			UserID:   setup.UserName,
			Password: setup.Password,
			PoolSize: 10,
			// Timeout:  1 * time.Second, // use default timeout
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		defer pool.Close()
		tasks := make([]*api.Task, 1)
		tasks[0] = &api.Task{Script: "sleep(62000);go;1+1"}
		err = pool.Execute(tasks)
		So(err, ShouldBeNil)
		if tasks[0].GetError() != nil {
			threadErr := tasks[0].GetError().Error()
			So(threadErr, ShouldContainSubstring, "timeout")
		}
	})
	Convey("TestConnnectionPooltimeOut_RefreshTimeout", t, func() {
		opt := &api.PoolOption{
			Address:  setup.Address4,
			UserID:   setup.UserName,
			Password: setup.Password,
			PoolSize: 10,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		pool.RefreshTimeout(1 * time.Second)

		defer pool.Close()
		tasks := make([]*api.Task, 10)
		for i := 0; i < 10; i++ {
			if i > 4 {
				tasks[i] = &api.Task{Script: "sleep(2000);go;1+1"}
				continue
			}
			tasks[i] = &api.Task{Script: "1+1"}
		}
		err = pool.Execute(tasks)
		So(err, ShouldBeNil)
		for i := 0; i < 10; i++ {
			succeed := false
			for {
				if tasks[i].IsSuccess(){
					succeed = true
					break
				}else{
					time.Sleep(3 * time.Second)
					break
				}
			}
			if succeed {
				re := tasks[i].GetResult()
				So(re.(*model.Scalar).Value().(int32), ShouldEqual, int32(2))
			}else{
				threadErr := tasks[i].GetError().Error()
				So(threadErr, ShouldContainSubstring, "timeout")
			}
		}
	})

	Convey("TestConnnectionPooltimeOut_exception", t, func() {
		opt := &api.PoolOption{
			Address:  setup.Address4,
			UserID:   setup.UserName,
			Password: setup.Password,
			PoolSize: 10,
			Timeout:  -100 * time.Second,
		}
		_, err := api.NewDBConnectionPool(opt)
		So(err.Error(), ShouldContainSubstring, "Timeout must be equal or greater than 0")
	})


}
