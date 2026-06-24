package test

import (
	"strconv"
	"testing"
	"time"

	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/dolphindb/api-go/v3/dolphindb"
	"github.com/dolphindb/api-go/v3/model"
	"github.com/dolphindb/api-go/v3/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

func testPoolOptions() *dolphindb.PoolOptions {
	return &dolphindb.PoolOptions{
		Address:     setup.Address,
		UserID:      setup.UserName,
		Password:    setup.Password,
		PoolSize:    10,
		LoadBalance: false,
	}
}

func TestNewTaskPool(t *testing.T) {
	t.Parallel()

	Convey("NewTaskPool_test", t, func() {
		Convey("Test_NewTaskPool_pool_size_not_set", func() {
			pool, err := dolphindb.NewTaskPool(&dolphindb.PoolOptions{
				Address:  setup.Address,
				UserID:   setup.UserName,
				Password: setup.Password,
				//PoolSize: 0,
			})
			So(pool, ShouldBeNil)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "PoolSize must be greater than 0")
		})

		Convey("Test_NewTaskPool_invalid_pool_size", func() {
			pool, err := dolphindb.NewTaskPool(&dolphindb.PoolOptions{
				Address:  setup.Address,
				UserID:   setup.UserName,
				Password: setup.Password,
				PoolSize: 0,
			})
			So(pool, ShouldBeNil)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "PoolSize must be greater than 0")
		})

		Convey("Test_NewTaskPool_success", func() {
			pool, err := dolphindb.NewTaskPool(testPoolOptions())
			So(err, ShouldBeNil)
			So(pool, ShouldNotBeNil)
			So(pool.GetPoolSize(), ShouldEqual, 10)
			So(pool.IsClosed(), ShouldBeFalse)

			err = pool.Close()
			So(err, ShouldBeNil)
			So(pool.IsClosed(), ShouldBeTrue)
		})
	})
}

func TestNewConnPool(t *testing.T) {
	t.Parallel()

	Convey("NewConnPool_method_test", t, func() {
		Convey("Test_NewConnPool_invalid_pool_size_0", func() {
			pool, err := dolphindb.NewConnPool(&dolphindb.PoolOptions{
				Address:  setup.Address,
				UserID:   setup.UserName,
				Password: setup.Password,
				PoolSize: 0,
			})
			So(pool, ShouldBeNil)
			So(pool.Size(), ShouldEqual, 0)
			So(pool.IsClosed(), ShouldEqual, true)

			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "PoolSize must be greater than 0")
		})

		Convey("Test_NewConnPool_invalid_pool_size_negative", func() {
			pool, err := dolphindb.NewConnPool(&dolphindb.PoolOptions{
				Address:  setup.Address,
				UserID:   setup.UserName,
				Password: setup.Password,
				PoolSize: -1,
			})
			So(pool, ShouldBeNil)
			So(pool.Size(), ShouldEqual, 0)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "PoolSize must be greater than 0")
		})

		SkipConvey("Test_NewConnPool_success_Acquire_gt_poolSize", func() {
			pool, err := dolphindb.NewConnPool(&dolphindb.PoolOptions{
				Address:  setup.Address,
				UserID:   setup.UserName,
				Password: setup.Password,
				PoolSize: 2,
			})
			So(err, ShouldBeNil)
			So(pool, ShouldNotBeNil)
			So(pool.Size(), ShouldEqual, 2)
			So(pool.IsClosed(), ShouldBeFalse)

			conn, err := pool.Acquire()
			conn1, err1 := pool.Acquire()
			So(conn, ShouldNotBeNil)
			So(conn1, ShouldNotBeNil)
			So(err, ShouldBeNil)
			So(err1, ShouldBeNil)
			_, err2 := pool.Acquire()
			So(err2, ShouldNotBeNil)
			So(err2.Error(), ShouldContainSubstring, "no connection available")
			err = pool.Close()
			So(err, ShouldBeNil)
			So(pool.IsClosed(), ShouldBeTrue)
		})

		Convey("Test_NewConnPool_nil_Acquire", func() {
			pool, err := dolphindb.NewConnPool(&dolphindb.PoolOptions{
				Address:  setup.Address,
				UserID:   setup.UserName,
				Password: setup.Password,
				PoolSize: 0,
			})
			So(err, ShouldNotBeNil)
			So(pool, ShouldBeNil)
			So(pool.Size(), ShouldEqual, 0)
			So(pool.IsClosed(), ShouldEqual, true)

			_, err = pool.Acquire()
			So(err.Error(), ShouldContainSubstring, "connection pool is closed")
			err = pool.Close()
			So(err, ShouldBeNil)
			So(pool.IsClosed(), ShouldBeTrue)
		})

		Convey("Test_NewConnPool_success_Acquire_RunScript", func() {
			pool, err := dolphindb.NewConnPool(testPoolOptions())
			So(err, ShouldBeNil)
			So(pool, ShouldNotBeNil)
			So(pool.Size(), ShouldEqual, 10)
			So(pool.IsClosed(), ShouldBeFalse)

			lease, err := pool.Acquire()
			So(pool.Size(), ShouldEqual, 9)
			So(err, ShouldBeNil)
			So(lease, ShouldNotBeNil)
			re, err := lease.Conn().RunScript("1+1")
			So(re.String(), ShouldEqual, "int(2)")
			So(err, ShouldBeNil)

			err = pool.Release(lease.Conn())
			So(err, ShouldBeNil)
			So(pool.Size(), ShouldEqual, 10)
			err = pool.Close()
			So(err, ShouldBeNil)
			So(pool.IsClosed(), ShouldBeTrue)
		})

		Convey("Test_NewConnPool_success_Acquire_Release_RunScript", func() {
			pool, err := dolphindb.NewConnPool(testPoolOptions())
			So(err, ShouldBeNil)
			So(pool, ShouldNotBeNil)
			So(pool.Size(), ShouldEqual, 10)
			So(pool.IsClosed(), ShouldBeFalse)

			lease, err := pool.Acquire()
			So(err, ShouldBeNil)
			So(lease, ShouldNotBeNil)

			err = pool.Release(lease.Conn())
			So(err, ShouldBeNil)
			//_, err = lease.Conn().RunScript("1+1")
			//So(err, ShouldNotBeNil)
			err = pool.Close()
			So(err, ShouldBeNil)
			So(pool.IsClosed(), ShouldBeTrue)
			So(pool.Size(), ShouldEqual, 0)
		})

		Convey("Test_NewConnPool_success_Acquire_Release_duplicate", func() {
			pool, err := dolphindb.NewConnPool(testPoolOptions())
			So(err, ShouldBeNil)
			So(pool, ShouldNotBeNil)
			So(pool.Size(), ShouldEqual, 10)
			So(pool.IsClosed(), ShouldBeFalse)

			lease, err := pool.Acquire()
			So(err, ShouldBeNil)
			So(lease, ShouldNotBeNil)

			err = pool.Release(lease.Conn())
			So(err, ShouldBeNil)
			//err = pool.Release(lease.Conn())
			//So(err, ShouldNotBeNil)

			err = pool.Close()
			So(err, ShouldBeNil)
			So(pool.IsClosed(), ShouldBeTrue)
			So(pool.Size(), ShouldEqual, 0)
		})

		Convey("Test_NewConnPool_success_Acquire_close_RunScript", func() {
			pool, err := dolphindb.NewConnPool(testPoolOptions())
			So(err, ShouldBeNil)
			So(pool, ShouldNotBeNil)
			So(pool.Size(), ShouldEqual, 10)
			So(pool.IsClosed(), ShouldBeFalse)

			lease, err := pool.Acquire()
			So(err, ShouldBeNil)
			So(lease, ShouldNotBeNil)

			err = pool.Close()
			So(err, ShouldBeNil)

			_, err = lease.Conn().RunScript("1+1")
			So(err, ShouldBeNil)

			So(pool.IsClosed(), ShouldBeTrue)
			So(pool.Size(), ShouldEqual, 0) //Size 空闲连接数
		})

		Convey("Test_NewConnPool_success_Acquire_Release_close_RunScript", func() {
			pool, err := dolphindb.NewConnPool(testPoolOptions())
			So(err, ShouldBeNil)
			So(pool, ShouldNotBeNil)
			So(pool.Size(), ShouldEqual, 10)
			So(pool.IsClosed(), ShouldBeFalse)

			lease, err := pool.Acquire()
			So(err, ShouldBeNil)
			So(lease, ShouldNotBeNil)

			err = pool.Release(lease.Conn())
			So(err, ShouldBeNil)

			err = pool.Close()
			So(err, ShouldBeNil)

			_, err = lease.Conn().RunScript("1+1")
			So(err, ShouldNotBeNil)

			So(pool.IsClosed(), ShouldBeTrue)
			So(pool.Size(), ShouldEqual, 0)
		})

		Convey("Test_NewConnPool_success_close_duplicate", func() {
			pool, err := dolphindb.NewConnPool(testPoolOptions())
			So(err, ShouldBeNil)

			err = pool.Close()
			So(err, ShouldBeNil)
			So(pool.IsClosed(), ShouldBeTrue)
			So(pool.Size(), ShouldEqual, 0)

			err = pool.Close()
			So(err, ShouldBeNil)
			So(pool.IsClosed(), ShouldBeTrue)
			So(pool.Size(), ShouldEqual, 0)
		})

		Convey("Test_NewConnPool_WithConn", func() {
			pool, err := dolphindb.NewConnPool(testPoolOptions())
			So(err, ShouldBeNil)
			So(pool, ShouldNotBeNil)
			So(pool.Size(), ShouldEqual, 10)
			So(pool.IsClosed(), ShouldBeFalse)

			pool.WithConn(func(conn dialer.Conn) error {
				re, err := conn.RunScript("1+1")
				So(re.String(), ShouldEqual, "int(2)")
				So(err, ShouldBeNil)
				So(pool.Size(), ShouldEqual, 9)
				return nil
			})
			So(pool.Size(), ShouldEqual, 10)
			err = pool.Close()
			So(err, ShouldBeNil)
			So(pool.IsClosed(), ShouldBeTrue)
		})

		Convey("Test_NewConnPool_RefreshTimeout_negative", func() {
			pool, err := dolphindb.NewConnPool(testPoolOptions())
			So(err, ShouldBeNil)
			So(pool, ShouldNotBeNil)
			So(pool.Size(), ShouldEqual, 10)
			So(pool.IsClosed(), ShouldBeFalse)

			pool.RefreshTimeout(time.Duration(-10) * time.Millisecond)
			err = pool.Close()
			So(err, ShouldBeNil)
		})

		Convey("Test_NewConnPool_RefreshTimeout", func() {
			pool, err := dolphindb.NewConnPool(testPoolOptions())
			So(err, ShouldBeNil)
			So(pool, ShouldNotBeNil)
			So(pool.Size(), ShouldEqual, 10)
			So(pool.IsClosed(), ShouldBeFalse)

			pool.RefreshTimeout(time.Duration(10) * time.Millisecond)
			So(pool.Size(), ShouldEqual, 10)
			lease, err := pool.Acquire()
			So(err, ShouldBeNil)
			So(lease, ShouldNotBeNil)
			So(pool.Size(), ShouldEqual, 9)
			_, err = lease.Conn().RunScript("sleep(8)")
			So(err, ShouldBeNil)

			_, err = lease.Conn().RunScript("sleep(11)")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "i/o timeout")
			err = pool.Close()
			So(err, ShouldBeNil)
		})
	})

	Convey("NewConnPool_TableWrite_test", t, func() {
		Convey("Test_NewConnPool_TableAppender", func() {
			pool, err := dolphindb.NewConnPool(testPoolOptions())
			So(err, ShouldBeNil)
			So(pool, ShouldNotBeNil)
			So(pool.Size(), ShouldEqual, 10)
			So(pool.IsClosed(), ShouldBeFalse)

			lease, err := pool.Acquire()
			So(err, ShouldBeNil)
			So(lease, ShouldNotBeNil)
			So(pool.Size(), ShouldEqual, 9)

			tbName := "test_ta_" + strconv.FormatInt(time.Now().UnixNano(), 36)
			_, err = lease.Conn().RunScript(tbName + ` = table(100:0, ` +
				`["cbool", "cchar", "cshort", "cint", "clong", "cdate", "cmonth", "ctime", "cminute", "csecond", "cdatetime", "ctimestamp", "cnanotime", "cnanotimestamp", "cdatehour", "cfloat", "cdouble", "csymbol", "cstring", "cuuid", "cint128", "cip"], ` +
				`[BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, DATEHOUR, FLOAT, DOUBLE, SYMBOL, STRING, UUID, INT128, IPADDR])`)
			So(err, ShouldBeNil)

			appender, err := dolphindb.NewTableAppender(&dolphindb.TableAppenderOptions{
				TableName: tbName,
				Conn:      lease.Conn(),
			})
			So(err, ShouldBeNil)

			now := time.Date(2024, 1, 15, 10, 30, 45, 123456789, time.UTC)

			cbool, err := model.NewDataTypeListFromRawData(model.DtBool, []bool{true})
			So(err, ShouldBeNil)
			cchar, err := model.NewDataTypeListFromRawData(model.DtChar, []byte{'X'})
			So(err, ShouldBeNil)
			cshort, err := model.NewDataTypeListFromRawData(model.DtShort, []int16{12345})
			So(err, ShouldBeNil)
			cint, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{98765})
			So(err, ShouldBeNil)
			clong, err := model.NewDataTypeListFromRawData(model.DtLong, []int64{9876543210})
			So(err, ShouldBeNil)
			cdate, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{now})
			So(err, ShouldBeNil)
			cmonth, err := model.NewDataTypeListFromRawData(model.DtMonth, []time.Time{now})
			So(err, ShouldBeNil)
			ctime, err := model.NewDataTypeListFromRawData(model.DtTime, []time.Time{now})
			So(err, ShouldBeNil)
			cminute, err := model.NewDataTypeListFromRawData(model.DtMinute, []time.Time{now})
			So(err, ShouldBeNil)
			csecond, err := model.NewDataTypeListFromRawData(model.DtSecond, []time.Time{now})
			So(err, ShouldBeNil)
			cdatetime, err := model.NewDataTypeListFromRawData(model.DtDatetime, []time.Time{now})
			So(err, ShouldBeNil)
			ctimestamp, err := model.NewDataTypeListFromRawData(model.DtTimestamp, []time.Time{now})
			So(err, ShouldBeNil)
			cnanotime, err := model.NewDataTypeListFromRawData(model.DtNanoTime, []time.Time{now})
			So(err, ShouldBeNil)
			cnanotimestamp, err := model.NewDataTypeListFromRawData(model.DtNanoTimestamp, []time.Time{now})
			So(err, ShouldBeNil)
			cdatehour, err := model.NewDataTypeListFromRawData(model.DtDateHour, []time.Time{now})
			So(err, ShouldBeNil)
			cfloat, err := model.NewDataTypeListFromRawData(model.DtFloat, []float32{3.14})
			So(err, ShouldBeNil)
			cdouble, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{2.71828})
			So(err, ShouldBeNil)
			csymbol, err := model.NewDataTypeListFromRawData(model.DtSymbol, []string{"sym_A"})
			So(err, ShouldBeNil)
			cstring, err := model.NewDataTypeListFromRawData(model.DtString, []string{"hello"})
			So(err, ShouldBeNil)
			cuuid, err := model.NewDataTypeListFromRawData(model.DtUUID, []string{"5d212a78-cc48-e3b1-4235-b4d91473ee87"})
			So(err, ShouldBeNil)
			cint128, err := model.NewDataTypeListFromRawData(model.DtInt128, []string{"e1671797c52e15f763380b45e841ec32"})
			So(err, ShouldBeNil)
			cip, err := model.NewDataTypeListFromRawData(model.DtIP, []string{"192.168.1.100"})
			So(err, ShouldBeNil)

			colNames := []string{"cbool", "cchar", "cshort", "cint", "clong", "cdate", "cmonth", "ctime", "cminute", "csecond", "cdatetime", "ctimestamp", "cnanotime", "cnanotimestamp", "cdatehour", "cfloat", "cdouble", "csymbol", "cstring", "cuuid", "cint128", "cip"}
			newtable, err := model.NewTable(colNames, []*model.Vector{
				model.NewVector(cbool), model.NewVector(cchar), model.NewVector(cshort), model.NewVector(cint), model.NewVector(clong),
				model.NewVector(cdate), model.NewVector(cmonth), model.NewVector(ctime), model.NewVector(cminute), model.NewVector(csecond),
				model.NewVector(cdatetime), model.NewVector(ctimestamp), model.NewVector(cnanotime), model.NewVector(cnanotimestamp), model.NewVector(cdatehour),
				model.NewVector(cfloat), model.NewVector(cdouble), model.NewVector(csymbol), model.NewVector(cstring),
				model.NewVector(cuuid), model.NewVector(cint128), model.NewVector(cip),
			})
			So(err, ShouldBeNil)

			_, err = appender.Append(newtable)
			So(err, ShouldBeNil)

			re, err := lease.Conn().RunScript(tbName)
			So(err, ShouldBeNil)
			resultTable := re.(*model.Table)
			So(resultTable.Rows(), ShouldEqual, 1)
			So(resultTable.GetColumnByName("cint").Data.ElementValue(0), ShouldEqual, int32(98765))
			So(resultTable.GetColumnByName("cdouble").Data.ElementValue(0), ShouldEqual, 2.71828)
			So(resultTable.GetColumnByName("csymbol").Data.ElementValue(0), ShouldEqual, "sym_A")

			So(pool.Size(), ShouldEqual, 9)
			err = pool.Release(lease.Conn())
			So(err, ShouldBeNil)
			So(pool.Size(), ShouldEqual, 10)

			err = pool.Close()
			So(err, ShouldBeNil)
			So(pool.IsClosed(), ShouldBeTrue)
		})
	})
}
