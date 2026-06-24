package test

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/dolphindb/api-go/v3/api"
	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/dolphindb/api-go/v3/logging"
	"github.com/dolphindb/api-go/v3/model"
	"github.com/dolphindb/api-go/v3/test/setup"
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

func GetConnectionNumByName() map[string]int32 {
	Table, _ := globalConn.RunScript("select connectionNum, name from rpc(getControllerAlias(), getClusterPerf) where mode = 0 or mode=4")
	tmpTable := Table.(*model.Table)
	connectionNumList := tmpTable.GetColumnByName("connectionNum").Data.Value()
	nameList := tmpTable.GetColumnByName("name").Data.Value()
	result := make(map[string]int32, len(nameList))
	for i := range nameList {
		result[nameList[i].(string)] = connectionNumList[i].(int32)
	}
	return result
}

func GetOriginConnNum() map[string]int32 {
	var OriginConnectionNum map[string]int32
	var i = 0
	for {
		OriginConnectionNum = GetConnectionNumByName()
		if OriginConnectionNum != nil && i == 9 {
			break
		}
		i++
	}
	time.Sleep(3 * time.Second)
	OriginConnectionNum = GetConnectionNumByName()
	return OriginConnectionNum
}

func CheckConnectionNum(OriginConnectionNum map[string]int32, poolSize int) bool {
	Table, _ := globalConn.RunScript("select connectionNum, name from rpc(getControllerAlias(), getClusterPerf) where mode = 0 or mode=4")
	tmpTable := Table.(*model.Table)
	connectionNumList := tmpTable.GetColumnByName("connectionNum").Data.Value()
	nameList := tmpTable.GetColumnByName("name").Data.Value()
	newConnectionNum := make(map[string]int32, len(nameList))
	for i := range nameList {
		newConnectionNum[nameList[i].(string)] = connectionNumList[i].(int32)
	}
	fmt.Printf("\nOriginConnection:%v\n", OriginConnectionNum)
	fmt.Printf("NewConnection:%v\n", newConnectionNum)
	if len(OriginConnectionNum) != len(newConnectionNum) {
		return false
	}
	base := int32(poolSize / len(OriginConnectionNum))
	remainder := poolSize % len(OriginConnectionNum)
	extraCount := 0
	for name, originCount := range OriginConnectionNum {
		newCount, ok := newConnectionNum[name]
		delta := newCount - originCount
		if !ok || (delta != base && delta != base+1) {
			return false
		}
		if delta == base+1 {
			extraCount++
		}
	}
	return extraCount == remainder
}

func getConnectionAddresses(pool *api.DBConnectionPool) []string {
	v := reflect.ValueOf(pool).Elem().FieldByName("connections")
	if !v.IsValid() || v.IsNil() {
		return nil
	}

	chanValue := reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).Elem()
	count := chanValue.Len()
	addresses := make([]string, 0, count)
	for i := 0; i < count; i++ {
		pc, ok := chanValue.Recv()
		if !ok {
			break
		}

		// DBConnectionPool 内部 channel 存的是 api.pooledConn，而不是 dialer.Conn。
		// pooledConn.conn 是未导出字段，直接 Interface() 可能拿不到；因此先把 pc 拷贝到一个可寻址的值上。
		pcAddr := reflect.New(pc.Type()).Elem()
		pcAddr.Set(pc)
		connField := pcAddr.FieldByName("conn")
		if !connField.IsValid() {
			chanValue.Send(pc)
			continue
		}
		if !connField.CanInterface() {
			if connField.CanAddr() {
				connField = reflect.NewAt(connField.Type(), unsafe.Pointer(connField.UnsafeAddr())).Elem()
			}
		}
		realConn, ok := connField.Interface().(dialer.Conn)
		if ok {
			addresses = append(addresses, realConn.GetTCPConn().RemoteAddr().String())
		}
		chanValue.Send(pc)
	}

	return addresses
}

func CheckPoolConnectionAddresses(pool *api.DBConnectionPool, expected map[string]int) bool {
	counts := make(map[string]int)
	for _, addr := range getConnectionAddresses(pool) {
		counts[addr]++
	}
	fmt.Printf("\nPoolAddresses:%v\n", counts)
	if len(counts) != len(expected) {
		return false
	}
	for addr, want := range expected {
		if counts[addr] != want {
			return false
		}
	}
	return true
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
			pool, err := api.NewDBConnectionPool(opt)
			So(err, ShouldNotBeNil)
			So(pool, ShouldBeNil)
		})
		Convey("Test_function_DBConnectionPool_bad_address_format_exception", func() {
			opt := &api.PoolOption{
				Address:     "bad-address",
				UserID:      setup.UserName,
				Password:    setup.Password,
				PoolSize:    2,
				LoadBalance: false,
			}
			pool, err := api.NewDBConnectionPool(opt)
			So(err, ShouldNotBeNil)
			So(pool, ShouldBeNil)
		})
		Convey("Test_function_DBConnectionPool_address_nil_exception \n", func() {
			opt := &api.PoolOption{
				Address:     "",
				UserID:      setup.UserName,
				Password:    setup.Password,
				PoolSize:    2,
				LoadBalance: false,
			}
			pool, err := api.NewDBConnectionPool(opt)
			So(err, ShouldNotBeNil)
			So(pool, ShouldBeNil)
		})
		Convey("Test_function_DBConnectionPool_wrong_userName_exception \n", func() {
			opt := &api.PoolOption{
				Address:     host1,
				UserID:      "rootn1",
				Password:    setup.Password,
				PoolSize:    2,
				LoadBalance: false,
			}
			pool, err := api.NewDBConnectionPool(opt)
			So(err, ShouldNotBeNil)
			So(pool, ShouldBeNil)
		})
		Convey("Test_function_DBConnectionPool_userName_null_exception \n", func() {
			opt := &api.PoolOption{
				Address:     host1,
				UserID:      "",
				Password:    setup.Password,
				PoolSize:    2,
				LoadBalance: false,
			}
			pool, err := api.NewDBConnectionPool(opt)
			So(err, ShouldNotBeNil)
			So(pool, ShouldBeNil)
		})
		Convey("Test_function_DBConnectionPool_wrong_Password_exception \n", func() {
			opt := &api.PoolOption{
				Address:     host1,
				UserID:      setup.UserName,
				Password:    "rpoot120@",
				PoolSize:    2,
				LoadBalance: false,
			}
			pool, err := api.NewDBConnectionPool(opt)
			So(err, ShouldNotBeNil)
			So(pool, ShouldBeNil)
		})
		Convey("Test_function_DBConnectionPool_wrong_Password_special_symbol_exception \n", func() {
			opt := &api.PoolOption{
				Address:     host1,
				UserID:      setup.UserName,
				Password:    "!!!!!",
				PoolSize:    2,
				LoadBalance: false,
			}
			pool, err := api.NewDBConnectionPool(opt)
			So(err, ShouldNotBeNil)
			So(pool, ShouldBeNil)
		})
		Convey("Test_function_DBConnectionPool_wrong_Password_exception_Reconnect_true \n", func() {
			t := 2
			opt := &api.PoolOption{
				Address:          host1,
				UserID:           setup.UserName,
				Password:         "rpoot120@",
				PoolSize:         10,
				LoadBalance:      false,
				Reconnect:        true,
				TryReconnectNums: &t,
			}
			pool, err := api.NewDBConnectionPool(opt)
			So(err, ShouldNotBeNil)
			So(pool, ShouldBeNil)
		})
		Convey("Test_function_DBConnectionPool_PoolSize_less_than_0_exception", func() {
			opt := &api.PoolOption{
				Address:     host1,
				UserID:      setup.UserName,
				Password:    setup.Password,
				PoolSize:    -1,
				LoadBalance: false,
			}
			pool, err := api.NewDBConnectionPool(opt)
			So(err, ShouldNotBeNil)
			So(pool, ShouldBeNil)
		})
		Convey("Test_function_DBConnectionPool_PoolSize_equal_0_exception", func() {
			opt := &api.PoolOption{
				Address:     host1,
				UserID:      setup.UserName,
				Password:    setup.Password,
				PoolSize:    0,
				LoadBalance: false,
			}
			pool, err := api.NewDBConnectionPool(opt)
			So(err, ShouldNotBeNil)
			So(pool, ShouldBeNil)
		})
		Convey("Test_function_DBConnectionPool_EnableHighAvailability_empty_sites_exception", func() {
			opt := &api.PoolOption{
				Address:                host1,
				UserID:                 setup.UserName,
				Password:               setup.Password,
				PoolSize:               2,
				LoadBalance:            false,
				EnableHighAvailability: true,
				HighAvailabilitySites:  nil,
			}
			pool, err := api.NewDBConnectionPool(opt)
			So(err, ShouldNotBeNil)
			So(pool, ShouldBeNil)
			So(err.Error(), ShouldContainSubstring, "if EnableHighAvailability is true, HighAvailabilitySites should be specified")
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
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "LoadBalanceAddresses requires LoadBalance to be true")
			So(pool, ShouldBeNil)
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

func TestDBConnectionPool_ExecuteTask(t *testing.T) {
	opt := &api.PoolOption{
		Address:     host1,
		UserID:      setup.UserName,
		Password:    setup.Password,
		PoolSize:    2,
		LoadBalance: false,
	}

	newPool := func() *api.DBConnectionPool {
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		So(pool, ShouldNotBeNil)
		So(pool.GetPoolSize(), ShouldEqual, 2)
		return pool
	}

	Convey("Test_function_DBConnectionPool_ExecuteTask_task_null", t, func() {
		pool := &api.DBConnectionPool{}
		err := pool.ExecuteTask(nil)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "task must not be nil")
	})

	Convey("Test_function_DBConnectionPool_ExecuteTask_script_only", t, func() {
		pool := newPool()
		defer func() {
			So(pool.Close(), ShouldBeNil)
		}()
		task := &api.Task{Script: "typestr"}
		err := pool.ExecuteTask(task)
		So(err, ShouldBeNil)
		So(task.GetError(), ShouldBeNil)
		So(task.IsSuccess(), ShouldBeTrue)
		task1 := &api.Task{Script: "t=table(1..10 as id, 11..20 as value);t;"}
		err1 := pool.ExecuteTask(task1)
		So(err1, ShouldBeNil)
		So(task1.GetResult(), ShouldNotBeNil)
		table, ok := task1.GetResult().(*model.Table)
		So(ok, ShouldBeTrue)
		So(table.Rows(), ShouldEqual, 10)
		So(table.Columns(), ShouldEqual, 2)
		So(pool.IsClosed(), ShouldBeFalse)
	})

	Convey("Test_function_DBConnectionPool_ExecuteTask_with_args", t, func() {
		pool := newPool()
		defer func() {
			So(pool.Close(), ShouldBeNil)
		}()

		dt, err := model.NewDataType(model.DtString, "true")
		So(err, ShouldBeNil)
		s := model.NewScalar(dt)
		task := &api.Task{
			Script: "typestr",
			Args:   []model.DataForm{s},
		}

		err = pool.ExecuteTask(task)
		So(err, ShouldBeNil)
		So(task.GetError(), ShouldBeNil)
		So(task.IsSuccess(), ShouldBeTrue)
		So(pool.IsClosed(), ShouldBeFalse)
	})

	Convey("Test_function_DBConnectionPool_ExecuteTask_task_error", t, func() {
		pool := newPool()
		defer func() {
			So(pool.Close(), ShouldBeNil)
		}()

		task := &api.Task{Script: "this_script_should_fail()"}

		err := pool.ExecuteTask(task)

		So(err, ShouldNotBeNil)
		So(task.GetError(), ShouldNotBeNil)
		So(task.IsSuccess(), ShouldBeFalse)
		So(pool.IsClosed(), ShouldBeFalse)
	})
}

func TestDBConnectionPoolClosedState(t *testing.T) {
	Convey("Test_function_DBConnectionPoolClosedState", t, func() {
		opt := &api.PoolOption{
			Address:     host1,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    1,
			LoadBalance: false,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeFalse)

		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)

		// _, err = pool.Acquire()
		// So(err, ShouldNotBeNil)
		// So(err.Error(), ShouldEqual, "connection pool is closed")

		// err = pool.Release(nil)
		// So(err, ShouldBeNil)

		task := &api.Task{Script: "typestr"}
		err = pool.ExecuteTask(task)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "connection pool is closed")
	})
}

func TestDBConnectionPool_Lifecycle(t *testing.T) {
	Convey("Test_function_DBConnectionPool_Lifecycle", t, func() {
		opt := &api.PoolOption{
			Address:     host1,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    2,
			LoadBalance: false,
		}

		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		So(pool, ShouldNotBeNil)
		So(pool.IsClosed(), ShouldBeFalse)

		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)

		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)

		cmd := exec.Command(os.Args[0], "-test.run=TestDBConnectionPool_ExecuteAfterClose_Process")
		cmd.Env = append(os.Environ(), "TEST_POOL_EXECUTE_AFTER_CLOSE=1")
		out, err := cmd.CombinedOutput()
		So(err, ShouldNotBeNil)
		So(string(out), ShouldContainSubstring, "panic")
		So(pool.IsClosed(), ShouldBeTrue)
	})
}

func TestDBConnectionPool_ExecuteAfterClose_Process(t *testing.T) {
	if os.Getenv("TEST_POOL_EXECUTE_AFTER_CLOSE") != "1" {
		return
	}

	opt := &api.PoolOption{
		Address:     host1,
		UserID:      setup.UserName,
		Password:    setup.Password,
		PoolSize:    2,
		LoadBalance: false,
	}

	pool, err := api.NewDBConnectionPool(opt)
	if err != nil {
		t.Fatal(err)
	}
	if err = pool.Close(); err != nil {
		t.Fatal(err)
	}

	task := &api.Task{Script: "typestr", Args: nil}
	_ = pool.Execute([]*api.Task{task})
}

func TestDBConnectionPool_LoadBalance(t *testing.T) {
	Convey("Test_function_DBConnectionPool_LoadBalance_false", t, func() {
		OriginConnectionNum := GetOriginConnNum()
		fmt.Printf("\norigin connection:%v\n", OriginConnectionNum)
		opt := &api.PoolOption{
			Address:     host1,
			UserID:      setup.UserName,
			Password:    setup.Password,
			PoolSize:    8,
			LoadBalance: false,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		re := pool.GetPoolSize()
		So(re, ShouldEqual, 8)
		addrOK := CheckPoolConnectionAddresses(pool, map[string]int{
			host1: 8,
		})
		So(addrOK, ShouldBeTrue)
		closed := pool.IsClosed()
		So(closed, ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		closed = pool.IsClosed()
		So(closed, ShouldBeTrue)
	})

	Convey("Test_function_DBConnectionPool_LoadBalance_false_EnableHighAvailability_true", t, func() {
		OriginConnectionNum := GetOriginConnNum()
		fmt.Printf("\norigin connection:%v\n", OriginConnectionNum)
		opt := &api.PoolOption{
			Address:                host1,
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               8,
			LoadBalance:            false,
			EnableHighAvailability: true,
			HighAvailabilitySites:  []string{setup.Address, setup.Address2, setup.Address3, setup.Address4},
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		re := pool.GetPoolSize()
		So(re, ShouldEqual, 8)
		addrOK := CheckPoolConnectionAddresses(pool, map[string]int{
			host1: 8,
		})
		So(addrOK, ShouldBeTrue)
		closed := pool.IsClosed()
		So(closed, ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		closed = pool.IsClosed()
		So(closed, ShouldBeTrue)
	})

	Convey("Test_function_DBConnectionPool_LoadBalance_true_not_SetLoadBalanceAddress", t, func() {
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
		addrOK := CheckPoolConnectionAddresses(pool, map[string]int{
			setup.Address:  2,
			setup.Address2: 2,
			setup.Address3: 2,
			setup.Address4: 2,
		})
		So(addrOK, ShouldBeTrue)
		closed := pool.IsClosed()
		So(closed, ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		closed = pool.IsClosed()
		So(closed, ShouldBeTrue)
	})

	Convey("Test_function_DBConnectionPool_LoadBalance_true_not_SetLoadBalanceAddress1", t, func() {
		OriginConnectionNum := GetOriginConnNum()
		fmt.Printf("\norigin connection:%v\n", OriginConnectionNum)
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
		addrOK := CheckPoolConnectionAddresses(pool, map[string]int{
			setup.Address:  2,
			setup.Address2: 2,
			setup.Address3: 3,
			setup.Address4: 3,
		})
		So(addrOK, ShouldBeTrue)
		closed := pool.IsClosed()
		So(closed, ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		closed = pool.IsClosed()
		So(closed, ShouldBeTrue)
	})
}
func TestDBConnectionPool_LoadBalance_true_SetLoadBalanceAddress(t *testing.T) {
	Convey("Test_function_DBConnectionPool_SetLoadBalanceAddress", t, func() {
		time.Sleep(3 * time.Second)
		OriginConnectionNum := GetOriginConnNum()
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
		allAddresses := []string{opt.Address, setup.Address, setup.Address2, setup.Address3, setup.Address4}
		expectedAddressCounts := make(map[string]int)
		uniqueAddresses := make([]string, 0, len(allAddresses))
		seenAddresses := make(map[string]struct{}, len(allAddresses))
		for _, addr := range allAddresses {
			if addr == "" {
				continue
			}
			if _, ok := seenAddresses[addr]; ok {
				continue
			}
			seenAddresses[addr] = struct{}{}
			uniqueAddresses = append(uniqueAddresses, addr)
		}
		baseCount := re / len(uniqueAddresses)
		remainderCount := re % len(uniqueAddresses)
		for i, addr := range uniqueAddresses {
			expectedAddressCounts[addr] = baseCount
			if i < remainderCount {
				expectedAddressCounts[addr]++
			}
		}
		addrOK := CheckPoolConnectionAddresses(pool, expectedAddressCounts)
		So(addrOK, ShouldBeTrue)
		closed := pool.IsClosed()
		So(closed, ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		closed = pool.IsClosed()
		So(closed, ShouldBeTrue)
	})

	Convey("Test_function_DBConnectionPool_LoadBalanceAddresses_not_contian_Address", t, func() {
		time.Sleep(3 * time.Second)
		OriginConnectionNum := GetOriginConnNum()
		fmt.Printf("\norigin connection:%v\n", OriginConnectionNum)
		opt := &api.PoolOption{
			Address:              setup.Address,
			UserID:               setup.UserName,
			Password:             setup.Password,
			PoolSize:             5,
			LoadBalance:          true,
			LoadBalanceAddresses: []string{setup.Address2, setup.Address3, setup.Address4},
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		re := pool.GetPoolSize()
		So(re, ShouldEqual, 5)
		allAddresses := []string{opt.Address, setup.Address, setup.Address2, setup.Address3, setup.Address4}
		expectedAddressCounts := make(map[string]int)
		uniqueAddresses := make([]string, 0, len(allAddresses))
		seenAddresses := make(map[string]struct{}, len(allAddresses))
		for _, addr := range allAddresses {
			if addr == "" {
				continue
			}
			if _, ok := seenAddresses[addr]; ok {
				continue
			}
			seenAddresses[addr] = struct{}{}
			uniqueAddresses = append(uniqueAddresses, addr)
		}
		baseCount := re / len(uniqueAddresses)
		remainderCount := re % len(uniqueAddresses)
		for i, addr := range uniqueAddresses {
			expectedAddressCounts[addr] = baseCount
			if i < remainderCount {
				expectedAddressCounts[addr]++
			}
		}
		addrOK := CheckPoolConnectionAddresses(pool, expectedAddressCounts)
		So(addrOK, ShouldBeTrue)
		closed := pool.IsClosed()
		So(closed, ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		closed = pool.IsClosed()
		So(closed, ShouldBeTrue)
	})

	Convey("Test_function_DBConnectionPool_LoadBalanceAddresses_not_contian_Address1", t, func() {
		time.Sleep(3 * time.Second)
		OriginConnectionNum := GetOriginConnNum()
		fmt.Printf("\norigin connection:%v\n", OriginConnectionNum)
		opt := &api.PoolOption{
			Address:              setup.Address,
			UserID:               setup.UserName,
			Password:             setup.Password,
			PoolSize:             5,
			LoadBalance:          true,
			LoadBalanceAddresses: []string{setup.Address3, setup.Address4},
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		re := pool.GetPoolSize()
		So(re, ShouldEqual, 5)
		allAddresses := []string{opt.Address, setup.Address, setup.Address3, setup.Address4}
		expectedAddressCounts := make(map[string]int)
		uniqueAddresses := make([]string, 0, len(allAddresses))
		seenAddresses := make(map[string]struct{}, len(allAddresses))
		for _, addr := range allAddresses {
			if addr == "" {
				continue
			}
			if _, ok := seenAddresses[addr]; ok {
				continue
			}
			seenAddresses[addr] = struct{}{}
			uniqueAddresses = append(uniqueAddresses, addr)
		}
		baseCount := re / len(uniqueAddresses)
		remainderCount := re % len(uniqueAddresses)
		for i, addr := range uniqueAddresses {
			expectedAddressCounts[addr] = baseCount
			if i < remainderCount {
				expectedAddressCounts[addr]++
			}
		}
		addrOK := CheckPoolConnectionAddresses(pool, expectedAddressCounts)
		So(addrOK, ShouldBeTrue)
		closed := pool.IsClosed()
		So(closed, ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		closed = pool.IsClosed()
		So(closed, ShouldBeTrue)
	})
}

func TestDBConnectionPool_LoadBalance_true_SetLoadBalanceAddress_EnableHighAvailability_true(t *testing.T) {
	Convey("TestDBConnectionPool_LoadBalance_true_SetLoadBalanceAddress_EnableHighAvailability_true", t, func() {
		time.Sleep(3 * time.Second)
		OriginConnectionNum := GetOriginConnNum()
		fmt.Printf("\norigin connection:%v\n", OriginConnectionNum)
		opt := &api.PoolOption{
			Address:                setup.Address2,
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               5,
			LoadBalance:            true,
			LoadBalanceAddresses:   []string{setup.Address, setup.Address2, setup.Address3, setup.Address4},
			EnableHighAvailability: true,
			HighAvailabilitySites:  []string{setup.Address, setup.Address2, setup.Address3, setup.Address4},
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		re := pool.GetPoolSize()
		So(re, ShouldEqual, 5)
		allAddresses := []string{opt.Address, setup.Address, setup.Address2, setup.Address3, setup.Address4}
		expectedAddressCounts := make(map[string]int)
		uniqueAddresses := make([]string, 0, len(allAddresses))
		seenAddresses := make(map[string]struct{}, len(allAddresses))
		for _, addr := range allAddresses {
			if addr == "" {
				continue
			}
			if _, ok := seenAddresses[addr]; ok {
				continue
			}
			seenAddresses[addr] = struct{}{}
			uniqueAddresses = append(uniqueAddresses, addr)
		}
		baseCount := re / len(uniqueAddresses)
		remainderCount := re % len(uniqueAddresses)
		for i, addr := range uniqueAddresses {
			expectedAddressCounts[addr] = baseCount
			if i < remainderCount {
				expectedAddressCounts[addr]++
			}
		}
		addrOK := CheckPoolConnectionAddresses(pool, expectedAddressCounts)
		So(addrOK, ShouldBeTrue)
		closed := pool.IsClosed()
		So(closed, ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		closed = pool.IsClosed()
		So(closed, ShouldBeTrue)
	})

	Convey("TestDBConnectionPool_LoadBalance_true_SetLoadBalanceAddress_EnableHighAvailability_true_not_set_HighAvailabilitySites", t, func() {
		time.Sleep(3 * time.Second)
		OriginConnectionNum := GetOriginConnNum()
		fmt.Printf("\norigin connection:%v\n", OriginConnectionNum)
		opt := &api.PoolOption{
			Address:                setup.Address2,
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               5,
			LoadBalance:            true,
			LoadBalanceAddresses:   []string{setup.Address, setup.Address2, setup.Address3, setup.Address4},
			EnableHighAvailability: true,
			//HighAvailabilitySites:  []string{setup.Address, setup.Address2, setup.Address3, setup.Address4},
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		re := pool.GetPoolSize()
		So(re, ShouldEqual, 5)
		allAddresses := []string{opt.Address, setup.Address, setup.Address2, setup.Address3, setup.Address4}
		expectedAddressCounts := make(map[string]int)
		uniqueAddresses := make([]string, 0, len(allAddresses))
		seenAddresses := make(map[string]struct{}, len(allAddresses))
		for _, addr := range allAddresses {
			if addr == "" {
				continue
			}
			if _, ok := seenAddresses[addr]; ok {
				continue
			}
			seenAddresses[addr] = struct{}{}
			uniqueAddresses = append(uniqueAddresses, addr)
		}
		baseCount := re / len(uniqueAddresses)
		remainderCount := re % len(uniqueAddresses)
		for i, addr := range uniqueAddresses {
			expectedAddressCounts[addr] = baseCount
			if i < remainderCount {
				expectedAddressCounts[addr]++
			}
		}
		addrOK := CheckPoolConnectionAddresses(pool, expectedAddressCounts)
		So(addrOK, ShouldBeTrue)
		closed := pool.IsClosed()
		So(closed, ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		closed = pool.IsClosed()
		So(closed, ShouldBeTrue)
	})
	//取Address+LoadBalanceAddresses+HighAvailabilitySites的并集
	Convey("TestDBConnectionPool_LoadBalance_true_LoadBalanceAddresses_not_same_HighAvailabilitySites", t, func() {
		time.Sleep(3 * time.Second)
		OriginConnectionNum := GetOriginConnNum()
		fmt.Printf("\norigin connection:%v\n", OriginConnectionNum)
		opt := &api.PoolOption{
			Address:                setup.Address2,
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               5,
			LoadBalance:            true,
			LoadBalanceAddresses:   []string{setup.Address, setup.Address2},
			EnableHighAvailability: true,
			HighAvailabilitySites:  []string{setup.Address3, setup.Address4},
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		re := pool.GetPoolSize()
		So(re, ShouldEqual, 5)
		allAddresses := []string{opt.Address, setup.Address, setup.Address2, setup.Address3, setup.Address4}
		expectedAddressCounts := make(map[string]int)
		uniqueAddresses := make([]string, 0, len(allAddresses))
		seenAddresses := make(map[string]struct{}, len(allAddresses))
		for _, addr := range allAddresses {
			if addr == "" {
				continue
			}
			if _, ok := seenAddresses[addr]; ok {
				continue
			}
			seenAddresses[addr] = struct{}{}
			uniqueAddresses = append(uniqueAddresses, addr)
		}
		baseCount := re / len(uniqueAddresses)
		remainderCount := re % len(uniqueAddresses)
		for i, addr := range uniqueAddresses {
			expectedAddressCounts[addr] = baseCount
			if i < remainderCount {
				expectedAddressCounts[addr]++
			}
		}
		addrOK := CheckPoolConnectionAddresses(pool, expectedAddressCounts)
		So(addrOK, ShouldBeTrue)
		closed := pool.IsClosed()
		So(closed, ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		closed = pool.IsClosed()
		So(closed, ShouldBeTrue)
	})
}

func TestDBConnectionPool_LoadBalance_true_EnableHighAvailability_true_part_datanode_disconnect(t *testing.T) {
	//如果LoadBalanceAddresses中某个节点是不可用 那目前连接这个节点的时候 会切换到HighAvailabilitySites配置的第一个节点 那这种情况  最后的节点不是平均分配
	SkipConvey("TestDBConnectionPool_LoadBalance_true_EnableHighAvailability_true_part_datanode_disconnect", t, func() {
		time.Sleep(3 * time.Second)
		//OriginConnectionNum := GetOriginConnNum()
		//fmt.Printf("\norigin connection:%v\n", OriginConnectionNum)
		opt := &api.PoolOption{
			Address:                setup.Address2,
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               10,
			LoadBalance:            true,
			LoadBalanceAddresses:   []string{setup.Address, setup.Address2, setup.Address3, setup.Address4},
			EnableHighAvailability: true,
			HighAvailabilitySites:  []string{setup.Address, setup.Address2, setup.Address3, setup.Address4},
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		re := pool.GetPoolSize()
		So(re, ShouldEqual, 10)
		allAddresses := []string{opt.Address, setup.Address, setup.Address2, setup.Address3, setup.Address4}
		expectedAddressCounts := make(map[string]int)
		uniqueAddresses := make([]string, 0, len(allAddresses))
		seenAddresses := make(map[string]struct{}, len(allAddresses))
		for _, addr := range allAddresses {
			if addr == "" {
				continue
			}
			if _, ok := seenAddresses[addr]; ok {
				continue
			}
			seenAddresses[addr] = struct{}{}
			uniqueAddresses = append(uniqueAddresses, addr)
		}
		baseCount := re / len(uniqueAddresses)
		remainderCount := re % len(uniqueAddresses)
		for i, addr := range uniqueAddresses {
			expectedAddressCounts[addr] = baseCount
			if i < remainderCount {
				expectedAddressCounts[addr]++
			}
		}
		addrOK := CheckPoolConnectionAddresses(pool, expectedAddressCounts)
		So(addrOK, ShouldBeTrue)
		closed := pool.IsClosed()
		So(closed, ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		closed = pool.IsClosed()
		So(closed, ShouldBeTrue)
	})
}

func TestDBConnectionPool_hash_hash_string(t *testing.T) {
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
		newtable, err := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		So(err, ShouldBeNil)
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
		newtable, err := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		So(err, ShouldBeNil)
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
		for i := 0; i < 10000; i++ {
			symarr = append(symarr, int32(i))
			datetimearr = append(datetimearr, time.Date(1969, time.Month(12), i, 23, i, 50, 000, time.UTC))
		}
		sym, err := model.NewDataTypeListFromRawData(model.DtInt, symarr)
		So(err, ShouldBeNil)
		datetimev, err := model.NewDataTypeListFromRawData(model.DtTimestamp, datetimearr)
		So(err, ShouldBeNil)
		newtable, err := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		So(err, ShouldBeNil)
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
		newtable, err := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		So(err, ShouldBeNil)
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
		newtable, err := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		So(err, ShouldBeNil)
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
		newtable, err := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		So(err, ShouldBeNil)
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
		newtable, err := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		So(err, ShouldBeNil)
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
		newtable, err := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		So(err, ShouldBeNil)
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
		newtable, err := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		So(err, ShouldBeNil)
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
		newtable, err := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		So(err, ShouldBeNil)
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
		newtable, err := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		So(err, ShouldBeNil)
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
		newtable, err := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		So(err, ShouldBeNil)
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
		newtable, err := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		So(err, ShouldBeNil)
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
		newtable, err := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		So(err, ShouldBeNil)
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
		newtable, err := model.NewTable([]string{"datev", "sym"}, []*model.Vector{model.NewVector(datetimev), model.NewVector(sym)})
		So(err, ShouldBeNil)
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

func TestValidatePartitionedTableAppenderOption(t *testing.T) {
	Convey("Test_function_ValidatePartitionedTableAppenderOption", t, func() {
		Convey("Test_function_ValidatePartitionedTableAppenderOption_option_nil", func() {
			_, err := api.NewPartitionedTableAppender(nil)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "partitioned table appender option must not be nil")
		})

		Convey("Test_function_ValidatePartitionedTableAppenderOption_pool_nil", func() {
			_, err := api.NewPartitionedTableAppender(&api.PartitionedTableAppenderOption{
				TableName:    "test",
				PartitionCol: "col1",
			})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "partitioned table appender connection pool must not be nil")
		})

		Convey("Test_function_ValidatePartitionedTableAppenderOption_not_set_table_name", func() {
			pool := CreateDBConnectionPool(2, false)
			_, err := api.NewPartitionedTableAppender(&api.PartitionedTableAppenderOption{
				Pool: pool,
				//TableName:    "",
				PartitionCol: "col1",
			})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "partitioned table appender table name must not be empty")
			pool.Close()
		})

		Convey("Test_function_ValidatePartitionedTableAppenderOption_empty_table_name", func() {
			pool := CreateDBConnectionPool(2, false)
			_, err := api.NewPartitionedTableAppender(&api.PartitionedTableAppenderOption{
				Pool:         pool,
				TableName:    "",
				PartitionCol: "col1",
			})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "partitioned table appender table name must not be empty")
			pool.Close()
		})

		Convey("Test_function_ValidatePartitionedTableAppenderOption_blank_table_name", func() {
			pool := CreateDBConnectionPool(2, false)
			_, err := api.NewPartitionedTableAppender(&api.PartitionedTableAppenderOption{
				Pool:         pool,
				TableName:    "   ",
				PartitionCol: "col1",
			})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "partitioned table appender table name must not be empty")
			pool.Close()
		})

		Convey("Test_function_ValidatePartitionedTableAppenderOption_not_set_partition_col", func() {
			pool := CreateDBConnectionPool(2, false)
			_, err := api.NewPartitionedTableAppender(&api.PartitionedTableAppenderOption{
				Pool:      pool,
				TableName: "test",
				//PartitionCol: "",
			})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "partitioned table appender partition column must not be empty")
			pool.Close()
		})

		Convey("Test_function_ValidatePartitionedTableAppenderOption_empty_partition_col", func() {
			pool := CreateDBConnectionPool(2, false)
			_, err := api.NewPartitionedTableAppender(&api.PartitionedTableAppenderOption{
				Pool:         pool,
				TableName:    "test",
				PartitionCol: "",
			})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "partitioned table appender partition column must not be empty")
			pool.Close()
		})

		Convey("Test_function_ValidatePartitionedTableAppenderOption_blank_partition_col", func() {
			pool := CreateDBConnectionPool(2, false)
			_, err := api.NewPartitionedTableAppender(&api.PartitionedTableAppenderOption{
				Pool:         pool,
				TableName:    "test",
				PartitionCol: "   ",
			})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "partitioned table appender partition column must not be empty")
			pool.Close()
		})
	})
}
func TestPartitionedTableAppender(t *testing.T) {
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
			newtable, err := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			So(err, ShouldBeNil)
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
			globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
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
			newtable, err := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			So(err, ShouldBeNil)
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
			globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
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
			newtable, err := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			So(err, ShouldBeNil)
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
			globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
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
			newtable, err := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			So(err, ShouldBeNil)
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
			globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
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
			newtable, err := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			So(err, ShouldBeNil)
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
			globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
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
			globalConn.RunScript("dropDatabase('" + dbpath + "')")
		})
	})
}

func TestDBConnectionPool_task(t *testing.T) {
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
		globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
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
		globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
	})
}

func TestTableAppender(t *testing.T) {
	Convey("Test_function_TableAppender_prepare", t, func() {
		Convey("Test_function_TableAppender_option_nil", func() {
			_, err := api.NewTableAppender(nil)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "table appender option must not be nil")
		})

		Convey("Test_function_TableAppender_conn_nil", func() {
			_, err := api.NewTableAppender(&api.TableAppenderOption{TableName: "pt"})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "table appender connection must not be nil")
		})

		Convey("Test_function_TableAppender_TableName_not_set", func() {
			_, err := api.NewTableAppender(&api.TableAppenderOption{Conn: globalConn})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "table appender table name must not be empty")
		})

		Convey("Test_function_TableAppender_range_int", func() {
			tb := "test_akldwjgof"
			_, err := globalConn.RunScript(tb + `= table(100:0, ["sym", "id", "datev", "price"],[SYMBOL, INT, DATE, DOUBLE])`)
			So(err, ShouldBeNil)
			appenderOpt := &api.TableAppenderOption{
				TableName: tb,
				Conn:      globalConn,
			}
			appender, err := api.NewTableAppender(appenderOpt)
			So(err, ShouldBeNil)
			sym, err := model.NewDataTypeListFromRawData(model.DtString, []string{"AAPL", "BLS", "DBKS", "NDLN", "DBKS"})
			So(err, ShouldBeNil)
			id, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{2, 10, 12, 22, 23})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{time.Date(1970, time.Month(1), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(3), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(10), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(5), 1, 1, 1, 0, 0, time.UTC)})
			So(err, ShouldBeNil)
			price, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{21.2, 4.4, 5.5, 2.3, 6.6})
			So(err, ShouldBeNil)
			newtable, err := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			So(err, ShouldBeNil)
			// fmt.Println(newtable)
			_, err = appender.Append(newtable)
			So(err, ShouldBeNil)
			re, err := globalConn.RunScript(tb)
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

		Convey("Test_function_TableAppender_schema_request_error", func() {
			conn, err := api.NewSimpleDolphinDBClient(context.TODO(), host1, setup.UserName, setup.Password)
			So(err, ShouldBeNil)
			defer conn.Close()

			_, err = api.NewTableAppender(&api.TableAppenderOption{
				TableName: "table_does_not_exist_12345",
				Conn:      conn,
			})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, " Cannot recognize the token table_does_not_exist_12345")
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
			appender, err := api.NewTableAppender(appenderOpt)
			So(err, ShouldBeNil)
			So(err, ShouldBeNil)
			sym, err := model.NewDataTypeListFromRawData(model.DtString, []string{"A1", "A2", "A3", "A4", "A5"})
			So(err, ShouldBeNil)
			id, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{2, 7, 12, 22, 24})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{time.Date(1970, time.Month(1), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(3), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(10), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(5), 1, 1, 1, 0, 0, time.UTC)})
			So(err, ShouldBeNil)
			price, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{21.2, 4.4, 5.5, 2.3, 6.6})
			So(err, ShouldBeNil)
			newtable, err := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			So(err, ShouldBeNil)
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
			So(globalConnx.IsClosed(), ShouldBeTrue)
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
			appender, err := api.NewTableAppender(appenderOpt)
			So(err, ShouldBeNil)
			So(err, ShouldBeNil)
			sym, err := model.NewDataTypeListFromRawData(model.DtString, []string{"A1", "A2", "A3", "A4", "A5"})
			So(err, ShouldBeNil)
			id, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{2, 7, 12, 22, 24})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{time.Date(1970, time.Month(1), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(3), 1, 1, 1, 0, 0, time.UTC), time.Date(1969, time.Month(10), 1, 1, 1, 0, 0, time.UTC), time.Date(1970, time.Month(5), 1, 1, 1, 0, 0, time.UTC)})
			So(err, ShouldBeNil)
			price, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{21.2, 4.4, 5.5, 2.3, 6.6})
			So(err, ShouldBeNil)
			newtable, err := model.NewTable([]string{"sym", "id", "datev", "price"}, []*model.Vector{model.NewVector(sym), model.NewVector(id), model.NewVector(datev), model.NewVector(price)})
			So(err, ShouldBeNil)
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
			_, err = globalConnx.RunScript("dropDatabase('" + DfsDBPath + "')")
			So(err, ShouldBeNil)
			err = pool.Close()
			So(err, ShouldBeNil)
			globalConnx.Close()
			So(globalConnx.IsClosed(), ShouldBeTrue)
		})
	})
}

func TestConnnectionPoolHighAvailability(t *testing.T) {
	t.SkipNow()
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
		connCtl, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.CtlAdress, setup.UserName, setup.Password)
		AssertNil(err)
		getnametask := api.Task{Script: "getNodeAlias()"}
		tasks := []*api.Task{&getnametask}

		err = poolHA.Execute(tasks)
		AssertNil(err)
		origin_node := tasks[0].GetResult()
		fmt.Println("now", origin_node.(*model.Scalar).Value().(string), "is connected, try to stop it")
		connCtl.RunScript("stopDataNode(`" + origin_node.(*model.Scalar).Value().(string) + ")")
		time.Sleep(2 * time.Second)
		fmt.Println("stop success, check if the origin connection click to another node")
		err = poolHA.Execute(tasks)
		AssertNil(err)
		So(tasks[0].GetResult().String(), ShouldNotEqual, origin_node.(*model.Scalar).Value().(string))
		fmt.Println("check passed, restart the origin node")
		_, err = connCtl.RunScript(
			"nodes = exec name from getClusterPerf() where state!=1 and mode !=1;" +
				"startDataNode(nodes);")
		AssertNil(err)
		time.Sleep(2 * time.Second)
		connCtl.Close()
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
	Convey("TestConnnectionPoolOption_reconnect", t, func() {
		reconnNum := 1
		opt := &api.PoolOption{
			Address:          host1,
			UserID:           setup.UserName,
			Password:         setup.Password,
			PoolSize:         4,
			Reconnect:        true,
			TryReconnectNums: &reconnNum,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		defer pool.Close()
		// stop one node
		connCtl, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.CtlAdress, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		getnametask := api.Task{Script: "getNodeAlias()"}
		tasks := []*api.Task{&getnametask}
		err = pool.Execute(tasks)
		So(err, ShouldBeNil)
		origin_node := tasks[0].GetResult()
		fmt.Println("now", origin_node.(*model.Scalar).Value().(string), "is connected, try to stop it")
		_, err = connCtl.RunScript("stopDataNode(`" + origin_node.(*model.Scalar).Value().(string) + ");sleep(1000)")
		So(err, ShouldBeNil)
		fmt.Println("stop successfully")

		time.Sleep(1 * time.Second)
		_, err = connCtl.RunScript(
			"startDataNode(exec name from getClusterPerf() where state!=1 and mode !=1);sleep(1000)")
		So(err, ShouldBeNil)
		fmt.Println("restart success, check if the connection is ok")
		err = pool.Execute(tasks)
		So(err, ShouldBeNil)
		re := tasks[0].GetResult()
		So(re.(*model.Scalar).Value().(string), ShouldEqual, origin_node.(*model.Scalar).Value().(string))
		time.Sleep(2 * time.Second)
	})

}

func TestConnnectionPoolOption(t *testing.T) {
	Convey("TestConnnectionPoolOption_timeoutOption", t, func() {
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
		// So(err, ShouldBeNil)
		So(tasks[0].GetError(), ShouldNotBeNil)
		So(tasks[0].GetError().Error(), ShouldContainSubstring, "timeout")
	})
	Convey("TestConnnectionPoolOption_RefreshTimeout", t, func() {
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
		for i := 0; i < 10; i++ {
			if i > 4 {
				So(tasks[i].GetError(), ShouldNotBeNil)
				So(tasks[i].GetError().Error(), ShouldContainSubstring, "timeout")
				continue
			}
			So(tasks[i].GetError(), ShouldBeNil)
		}
		for i := 0; i < 10; i++ {
			succeed := false
			for {
				if tasks[i].IsSuccess() {
					succeed = true
					break
				} else {
					time.Sleep(3 * time.Second)
					break
				}
			}
			if succeed {
				re := tasks[i].GetResult()
				So(re.(*model.Scalar).Value().(int32), ShouldEqual, int32(2))
			} else {
				threadErr := tasks[i].GetError().Error()
				So(threadErr, ShouldContainSubstring, "timeout")
			}
		}
	})

	Convey("TestConnnectionPoolOption_exception", t, func() {
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

	Convey("Test_BehaviorOptions_timeout_reached", t, func() {
		timeout := time.Second * 2
		opt := &api.PoolOption{
			Timeout:  timeout,
			Address:  host1,
			UserID:   setup.UserName,
			Password: setup.Password,
			PoolSize: 10,
		}

		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		defer pool.Close()
		tasks := make([]*api.Task, 1)
		tasks[0] = &api.Task{Script: "sleep(30000);go;1+1"}
		start := time.Now()
		err = pool.Execute(tasks)
		// So(err, ShouldBeNil)
		So(tasks[0].GetError(), ShouldNotBeNil)
		So(tasks[0].GetError().Error(), ShouldContainSubstring, "timeout")
		end := time.Now()
		So(end.Sub(start).Seconds(), ShouldBeGreaterThanOrEqualTo, 2)
	})

	Convey("Test_BehaviorOptions_timeout_not_reached", t, func() {
		timeout := time.Second * 2
		opt := &api.PoolOption{
			Timeout:  timeout,
			Address:  host1,
			UserID:   setup.UserName,
			Password: setup.Password,
			PoolSize: 10,
		}

		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		defer pool.Close()
		tasks := make([]*api.Task, 1)
		tasks[0] = &api.Task{Script: "sleep(1000);go;1+1"}
		start := time.Now()
		err = pool.Execute(tasks)
		So(err, ShouldBeNil)
		end := time.Now()
		So(end.Sub(start).Seconds(), ShouldBeLessThan, 2)
		So(math.Abs(end.Sub(start).Seconds()-1), ShouldBeLessThan, 0.002)
		So(tasks[0].GetResult().(*model.Scalar).Value().(int32), ShouldEqual, int32(2))
	})
}

func Test_DBConnectionPool_SCRAM(t *testing.T) {
	db, err := api.NewSimpleDolphinDBClient(context.TODO(), host1, "admin", "123456")
	AssertNil(err)
	defer db.Close()
	_, err = db.RunScript("try{deleteUser('scramUser')}catch(ex){};go;createUser(`scramUser, `123456, authMode='scram')")
	if err != nil {
		t.Skip("skip test because create SCRAM user failed")
	}
	Convey("Test_DBConnectionPool_with_invalid_user", t, func() {
		opt := &api.PoolOption{
			EnableScram: true,
			Address:     host1,
			UserID:      "admin",
			Password:    "123456",
			PoolSize:    10,
		}
		_, err := api.NewDBConnectionPool(opt)
		So(err.Error(), ShouldContainSubstring, "user 'admin' doesn't support scram authMode")
	})
	Convey("Test_DBConnectionPool_SCRAM_login_success", t, func() {
		opt := &api.PoolOption{
			EnableScram: true,
			Address:     host1,
			UserID:      "scramUser",
			Password:    "123456",
			PoolSize:    10,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		defer pool.Close()
		tasks := make([]*api.Task, 1)
		tasks[0] = &api.Task{Script: "1+1"}
		err = pool.Execute(tasks)
		So(err, ShouldBeNil)
		So(tasks[0].GetResult().(*model.Scalar).Value().(int32), ShouldEqual, int32(2))
		pool.Close()
		So(pool.IsClosed(), ShouldBeTrue)
	})

}

func TestPartitionedTableAppender_SCRAM(t *testing.T) {
	db, err := api.NewSimpleDolphinDBClient(context.TODO(), host1, "admin", "123456")
	AssertNil(err)
	defer db.Close()
	_, err = db.RunScript("try{deleteUser('scramUser')}catch(ex){};go;createUser(`scramUser, `123456, authMode='scram')")
	if err != nil {
		t.Skip("skip test because create SCRAM user failed")
	}
	Convey("TestPartitionedTableAppender_SCRAM_login_success", t, func() {
		dbname := generateRandomString(8)
		_, err := globalConn.RunScript(`
				dbPath = "dfs://` + dbname + `"
				if(existsDatabase(dbPath))
					dropDatabase(dbPath)
				t = table(100:100, ["sym", "id", "datev", "price"], [SYMBOL, INT, DATE, DOUBLE])
				db = database(dbPath, VALUE, symbol("A"+string(1..6)))
				pt = db.createPartitionedTable(t, "pt", "sym")
				grant("scramUser", TABLE_READ, "dfs://` + dbname + `/pt")
				grant("scramUser", TABLE_WRITE, "dfs://` + dbname + `/pt")
			`)
		So(err, ShouldBeNil)
		opt := &api.PoolOption{
			EnableScram: true,
			Address:     setup.Address,
			UserID:      "scramUser",
			Password:    "123456",
			PoolSize:    10,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		defer pool.Close()
		data, err := globalConn.RunScript("t = table(take(`A1, 10) as sym, 1..10 as id, date(2020.01.01)+0..9 as datev, 1.1+0..9 as price)\n" +
			"t")
		So(err, ShouldBeNil)
		appenderOpt := &api.PartitionedTableAppenderOption{
			Pool:         pool,
			DBPath:       "dfs://" + dbname,
			TableName:    "pt",
			PartitionCol: "sym",
		}
		appender, err := api.NewPartitionedTableAppender(appenderOpt)
		So(err, ShouldBeNil)
		rows, err := appender.Append(data.(*model.Table))
		So(err, ShouldBeNil)
		So(rows, ShouldEqual, 10)
		err = appender.Close()
		So(err, ShouldBeNil)
		globalConn.RunScript("dropDatabase('dfs://" + dbname + "')")
	})
}

func TestTableAppender_SCRAM(t *testing.T) {
	db, err := api.NewSimpleDolphinDBClient(context.TODO(), host1, "admin", "123456")
	AssertNil(err)
	defer db.Close()
	_, err = db.RunScript("try{deleteUser('scramUser')}catch(ex){};go;createUser(`scramUser, `123456, authMode='scram')")
	if err != nil {
		t.Skip("skip test because create SCRAM user failed")
	}
	Convey("TestTableAppender_SCRAM_login_success", t, func() {
		data, _ := globalConn.RunScript("t = table(1..1000 as c1, rand(100.00, 1000) as c2);share table(1:0, `c1`c2, [INT, DOUBLE]) as t2; t")
		conn, err := api.NewSimpleDolphinDBClient(context.TODO(), host1, "scramUser", "123456")
		AssertNil(err)
		appenderOpt := &api.TableAppenderOption{
			Conn:      conn,
			TableName: "t2",
		}
		appender, err := api.NewTableAppender(appenderOpt)
		So(err, ShouldBeNil)
		So(err, ShouldBeNil)
		_, err = appender.Append(data.(*model.Table))
		So(err, ShouldBeNil)
		res, _ := globalConn.RunScript("res = select * from t2 order by c1;ex = select * from t order by c1;all(each(eqObj, res.values(), ex.values()))")
		So(res.(*model.Scalar).Value().(bool), ShouldBeTrue)
		err = appender.Close()
		So(err, ShouldBeNil)
		globalConn.RunScript("undef(`t2, SHARED)")
	})
}

// https://dolphindb1.atlassian.net/browse/AG-175
func TestConnnectionPoolOption_reconnect_true_TryReconnectNums(t *testing.T) {
	Convey("TestConnnectionPoolOption_reconnect_true_TryReconnectNums_negative", t, func() {
		reconnNum := -10
		opt := api.PoolOption{
			Address:          setup.CtlAdress,
			UserID:           setup.UserName,
			Password:         setup.Password,
			PoolSize:         10,
			Reconnect:        true,
			TryReconnectNums: &reconnNum,
		}
		_, err := api.NewDBConnectionPool(&opt)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "TryReconnectNums must be nil or greater than 0")
	})

	Convey("TestConnnectionPoolOption_reconnect_true_TryReconnectNums_0", t, func() {
		reconnNum := 0
		opt := api.PoolOption{
			Address:          setup.CtlAdress,
			UserID:           setup.UserName,
			Password:         setup.Password,
			PoolSize:         10,
			Reconnect:        true,
			TryReconnectNums: &reconnNum,
		}
		_, err := api.NewDBConnectionPool(&opt)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "TryReconnectNums must be nil or greater than 0")
	})

	Convey("TestConnnectionPoolOption_reconnect_true_TryReconnectNums_ConnectionNum", t, func() {
		connCtl, _ := api.NewSimpleDolphinDBClient(context.TODO(), setup.CtlAdress, setup.UserName, setup.Password)
		time.Sleep(2 * time.Second)
		res, _ := connCtl.RunScript("select connectionNum  from getClusterPerf(true) where port = " + strconv.Itoa(setup.CtlPort))
		connectionNum := res.(*model.Table).GetColumnByIndex(0).Get(0).Value()
		reconnNum := 10
		opt := api.PoolOption{
			Address:          setup.CtlAdress,
			UserID:           setup.UserName,
			Password:         setup.Password,
			PoolSize:         10,
			Reconnect:        true,
			TryReconnectNums: &reconnNum,
		}
		poll, err := api.NewDBConnectionPool(&opt)
		So(err, ShouldBeNil)
		So(poll.GetPoolSize(), ShouldEqual, 10)
		time.Sleep(2 * time.Second)
		res1, _ := connCtl.RunScript("select connectionNum  from getClusterPerf(true) where port = " + strconv.Itoa(setup.CtlPort))
		connectionNum1 := res1.(*model.Table).GetColumnByIndex(0).Get(0).Value()
		num1 := connectionNum.(int32)
		num2 := connectionNum1.(int32)
		So(num2-num1, ShouldEqual, 10)
	})
}

func TestConnnectionPoolOption_SqlStd(t *testing.T) {
	Convey("TestConnnectionPoolOption_SqlStd", t, func() {
		cases := []struct {
			name       string
			SqlStd     int
			shouldFail bool
		}{
			{name: "default_dolphindb", SqlStd: 0, shouldFail: true},
			{name: "oracle", SqlStd: 1, shouldFail: false},
			{name: "mysql", SqlStd: 2, shouldFail: false},
		}

		for _, tc := range cases {
			tc := tc
			Convey(tc.name, func() {
				opt := &api.PoolOption{
					Address:  host1,
					UserID:   setup.UserName,
					Password: setup.Password,
					PoolSize: 1,
					SqlStd:   dialer.SqlStdEnum(tc.SqlStd),
				}

				pool, err := api.NewDBConnectionPool(opt)
				So(err, ShouldBeNil)
				defer pool.Close()

				task := &api.Task{Script: "sysdate()"}
				err = pool.Execute([]*api.Task{task})

				if tc.shouldFail {
					So(task.GetError(), ShouldNotBeNil)
					So(task.GetError().Error(), ShouldContainSubstring, "sysdate")
					return
				} else {
					So(err, ShouldBeNil)
				}

				So(task.GetError(), ShouldBeNil)
				So(task.IsSuccess(), ShouldBeTrue)
				So(task.GetResult(), ShouldNotBeNil)
			})
		}
	})
}

func TestConnnectionPoolOption_NetTimeout(t *testing.T) {
	Convey("TestConnnectionPoolOption_NetTimeout_negative", t, func() {
		opt := &api.PoolOption{
			Address:    setup.Address4,
			UserID:     setup.UserName,
			Password:   setup.Password,
			PoolSize:   10,
			NetTimeout: -1 * time.Second,
		}
		_, err := api.NewDBConnectionPool(opt)
		So(err.Error(), ShouldContainSubstring, "the NetTimeout must be non-negative")
	})
	Convey("TestConnnectionPoolOption_NetTimeout_not_set", t, func() {
		opt := &api.PoolOption{
			Address:    setup.Address4,
			UserID:     setup.UserName,
			Password:   setup.Password,
			PoolSize:   10,
			NetTimeout: 0 * time.Second,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		So(pool, ShouldNotBeNil)
	})

	Convey("TestConnnectionPoolOption_NetTimeout_0", t, func() {
		opt := &api.PoolOption{
			Address:    setup.Address4,
			UserID:     setup.UserName,
			Password:   setup.Password,
			PoolSize:   10,
			NetTimeout: 0 * time.Second,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		So(pool, ShouldNotBeNil)
	})
}

func TestDBConnectionPool_Address_disconnection(t *testing.T) {
	logging.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	SkipConvey("TestDBConnectionPool_Address_disconnection_EnableHighAvailability_true_TryReconnectNums_nil", t, func() {
		//
		time.Sleep(3 * time.Second)
		//reconnNum := 3
		opt := &api.PoolOption{
			Address:  "192.168.0.69:7100",
			UserID:   setup.UserName,
			Password: setup.Password,
			PoolSize: 5,
			Timeout:  time.Second * 2,
			//Reconnect:              true,
			TryReconnectNums: nil,
			//LoadBalance:            true,
			//LoadBalanceAddresses:   []string{setup.Address2, setup.Address3, setup.Address4},
			EnableHighAvailability: true,
			HighAvailabilitySites:  []string{"192.168.0.69:7200"},
		}
		api.NewDBConnectionPool(opt)
		//无限重连
	})

	SkipConvey("TestDBConnectionPool_Address_disconnection_EnableHighAvailability_true_TryReconnectNums_notSet", t, func() {
		//
		time.Sleep(3 * time.Second)
		//reconnNum := 3
		opt := &api.PoolOption{
			Address:  "192.168.0.69:7100",
			UserID:   setup.UserName,
			Password: setup.Password,
			PoolSize: 5,
			Timeout:  time.Second * 2,
			//Reconnect:              true,
			//TryReconnectNums: &reconnNum,
			//LoadBalance:            true,
			//LoadBalanceAddresses:   []string{setup.Address2, setup.Address3, setup.Address4},
			EnableHighAvailability: true,
			HighAvailabilitySites:  []string{"192.168.0.69:7200"},
		}
		api.NewDBConnectionPool(opt)
		//无限重连
	})
	SkipConvey("TestDBConnectionPool_Address_disconnection_EnableHighAvailability_true_TryReconnectNums_5", t, func() {
		//
		time.Sleep(3 * time.Second)
		reconnNum := 5
		opt := &api.PoolOption{
			Address:  "192.168.0.69:7100",
			UserID:   setup.UserName,
			Password: setup.Password,
			PoolSize: 5,
			Timeout:  time.Second * 2,
			//Reconnect:              true,
			TryReconnectNums: &reconnNum,
			//LoadBalance:            true,
			//LoadBalanceAddresses:   []string{setup.Address2, setup.Address3, setup.Address4},
			EnableHighAvailability: true,
			HighAvailabilitySites:  []string{"192.168.0.69:7200"},
		}
		api.NewDBConnectionPool(opt)
		//重连5次后不再重连
	})

	Convey("TestDBConnectionPool_Address_stop_start_EnableHighAvailability_true", t, func() {
		time.Sleep(3 * time.Second)
		opt := &api.PoolOption{
			Address:  setup.Address,
			UserID:   setup.UserName,
			Password: setup.Password,
			PoolSize: 5,
			Timeout:  time.Second * 2,
			//Reconnect:              true,
			//TryReconnectNums: &reconnNum,
			//LoadBalance:            true,
			//LoadBalanceAddresses:   []string{setup.Address2, setup.Address3, setup.Address4},
			EnableHighAvailability: true,
			HighAvailabilitySites:  []string{setup.Address2},
		}

		connCtl, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.CtlAdress, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		So(connCtl, ShouldNotBeNil)

		connAddr1, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		So(connAddr1, ShouldNotBeNil)
		connAddr2, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address2, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		So(connAddr2, ShouldNotBeNil)

		addr1AliasRes, err := connAddr1.RunScript("getNodeAlias()")
		So(err, ShouldBeNil)
		addr1Alias := addr1AliasRes.(*model.Scalar).Value().(string)
		addr2AliasRes, err := connAddr2.RunScript("getNodeAlias()")
		So(err, ShouldBeNil)
		addr2Alias := addr2AliasRes.(*model.Scalar).Value().(string)

		err = connAddr1.Close()
		So(err, ShouldBeNil)
		err = connAddr2.Close()
		So(err, ShouldBeNil)

		fmt.Println("stop", addr1Alias, "and", addr2Alias)
		_, err = connCtl.RunScript("stopDataNode(`" + addr1Alias + ")")
		So(err, ShouldBeNil)
		_, err = connCtl.RunScript("stopDataNode(`" + addr2Alias + ")")
		So(err, ShouldBeNil)
		time.Sleep(10 * time.Second)

		go func() {
			time.Sleep(time.Minute)
			_, _ = connCtl.RunScript("startDataNode(`" + addr1Alias + ")")
			_, _ = connCtl.RunScript("startDataNode(`" + addr2Alias + ")")
		}()

		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		So(pool, ShouldNotBeNil)
		So(pool.GetPoolSize(), ShouldEqual, 5)
		So(pool.IsClosed(), ShouldBeFalse)

		execTask := &api.Task{Script: "a=1;\n a"}
		err = pool.Execute([]*api.Task{execTask})
		So(err, ShouldBeNil)
		So(execTask.GetError(), ShouldBeNil)
		So(execTask.IsSuccess(), ShouldBeTrue)
		So(execTask.GetResult(), ShouldNotBeNil)

		So(pool.IsClosed(), ShouldBeFalse)
		err = pool.Close()
		So(err, ShouldBeNil)
		So(pool.IsClosed(), ShouldBeTrue)
		err = connCtl.Close()
		So(err, ShouldBeNil)
	})
}

func TestDBConnectionPool_tableInsert_haStreamTable(t *testing.T) {
	Convey("TestDBConnectionPool_tableInsert_haStreamTable", t, func() {
		opt := &dialer.BehaviorOptions{
			EnableHighAvailability: true,
			HighAvailabilitySites:  []string{setup.Address, setup.Address2, setup.Address3},
		}

		connection, err := api.NewDolphinDBClient(context.TODO(), setup.Address, opt)
		So(err, ShouldBeNil)
		So(connection, ShouldNotBeNil)
		defer connection.Close()

		err = connection.Connect()
		So(err, ShouldBeNil)

		_, err = connection.RunScript("try{dropStreamTable(\"st_scada_value\")}catch(ex){}\ngo;\nt = table(1:0, `time`value`quality`flags`id`station`type, [TIMESTAMP,DOUBLE,INT,INT,SYMBOL,SYMBOL,SYMBOL]);\nhaStreamTable(11,t,`st_scada_value,100000);")
		So(err, ShouldBeNil)

		tmp, err := connection.RunScript("re = table(timestamp(1..10) as time, double(1..10) as value, 1..10 as quality,1..10 as flags, 'id'+string(1..10) as id, 'station'+string(1..10) as station, 'type'+string(1..10) as type); re;")
		So(err, ShouldBeNil)
		So(tmp, ShouldNotBeNil)

		values := []model.DataForm{tmp}
		pool, err := api.NewDBConnectionPool(&api.PoolOption{
			Address:                setup.Address,
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               3,
			EnableHighAvailability: true,
			HighAvailabilitySites:  []string{setup.Address, setup.Address2, setup.Address3},
		})
		So(err, ShouldBeNil)
		So(pool, ShouldNotBeNil)
		defer pool.Close()

		task := &api.Task{Script: "tableInsert{st_scada_value}", Args: values}
		//fmt.Println("---------------------------------Read data end------------------------------------")
		time.Sleep(3 * time.Second)
		//fmt.Println("Start Write!!!!!!!!!!!!!!!!!")
		for i := 0; i < 10; i++ {
			err = pool.ExecuteTask(task)
			So(err, ShouldBeNil)
			fmt.Println("数据插入", i, "次")
		}
		time.Sleep(5 * time.Second)
		res, err := connection.RunScript("select count(*) from st_scada_value")
		So(err, ShouldBeNil)
		So(res, ShouldNotBeNil)
		fmt.Println("The result is:\n", res.String())
		So(res.String(), ShouldContainSubstring, "100")
	})
}

func TestDBConnectionPool_tableInsert_haMvccTable_leader(t *testing.T) {
	Convey("TestDBConnectionPool_tableInsert_haMvccTable_leader", t, func() {
		conn, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)
		defer conn.Close()

		leaderRes, err := conn.RunScript(" exec port from rpc(getControllerAlias(), getClusterPerf) where name=getHaMvccLeader(3);\n")
		So(err, ShouldBeNil)
		leaderPort := int(leaderRes.(*model.Vector).Get(0).Value().(int32))

		pool, err := api.NewDBConnectionPool(&api.PoolOption{
			Address:                setup.IP + ":" + strconv.Itoa(leaderPort),
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               1,
			EnableHighAvailability: true,
			HighAvailabilitySites:  setup.HA_sites,
		})
		So(err, ShouldBeNil)
		So(pool, ShouldNotBeNil)
		defer pool.Close()

		tmp, err := conn.RunScript("table(1..100 as intv,take(`qq`ee`rr,100) as symbolv)")
		So(err, ShouldBeNil)
		So(tmp, ShouldNotBeNil)

		values := []model.DataForm{tmp}
		createTask := &api.Task{Script: "try{dropHaMvccTable(\"HaMvccTable1\")}catch(ex){};\n go;\n haMvccTable(1:0, table(array(INT) as intv,array(SYMBOL) as symbolv),\"HaMvccTable1\",3)"}
		err = pool.ExecuteTask(createTask)
		So(err, ShouldBeNil)

		time.Sleep(3 * time.Second)
		insertTask := &api.Task{Script: "tableInsert{loadHaMvccTable('HaMvccTable1')}", Args: values}
		err = pool.ExecuteTask(insertTask)
		So(err, ShouldBeNil)

		checkTask := &api.Task{Script: "each(eqObj, (select * from loadHaMvccTable('HaMvccTable1')).values(), table(1..100 as intv,take(`qq`ee`rr,100) as symbolv).values()).all()"}
		err = pool.ExecuteTask(checkTask)
		So(err, ShouldBeNil)
		So(checkTask.GetResult().(*model.Scalar).Value().(bool), ShouldBeTrue)
	})
}

func TestDBConnectionPool_tableInsert_haMvccTable_follower(t *testing.T) {
	Convey("TestDBConnectionPool_tableInsert_haMvccTable_follower", t, func() {
		conn, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)
		defer conn.Close()

		leaderRes, err := conn.RunScript(" exec port from rpc(getControllerAlias(), getClusterPerf) where name=getHaMvccLeader(3);\n")
		So(err, ShouldBeNil)
		leaderPort := int(leaderRes.(*model.Vector).Get(0).Value().(int32))

		followerRes, err := conn.RunScript(" exec port from rpc(getControllerAlias(), getClusterPerf) where name in (exec sites[0] from getHaMvccRaftGroups() where id==3).split(\",\") and name!=getHaMvccLeader(3) limit 1;\n")
		So(err, ShouldBeNil)
		followerPort := int(followerRes.(*model.Vector).Get(0).Value().(int32))

		leaderPool, err := api.NewDBConnectionPool(&api.PoolOption{
			Address:                setup.IP + ":" + strconv.Itoa(leaderPort),
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               1,
			EnableHighAvailability: true,
			HighAvailabilitySites:  setup.HA_sites,
		})
		So(err, ShouldBeNil)
		So(leaderPool, ShouldNotBeNil)
		defer leaderPool.Close()

		followerPool, err := api.NewDBConnectionPool(&api.PoolOption{
			Address:                setup.IP + ":" + strconv.Itoa(followerPort),
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               1,
			EnableHighAvailability: true,
			HighAvailabilitySites:  setup.HA_sites,
		})
		So(err, ShouldBeNil)
		So(followerPool, ShouldNotBeNil)
		defer followerPool.Close()

		tmp, err := conn.RunScript("table(1..100 as intv,take(`qq`ee`rr,100) as symbolv)")
		So(err, ShouldBeNil)
		So(tmp, ShouldNotBeNil)

		values := []model.DataForm{tmp}
		createTask := &api.Task{Script: "try{dropHaMvccTable(\"HaMvccTable1\")}catch(ex){};\n go;\n haMvccTable(1:0, table(array(INT) as intv,array(SYMBOL) as symbolv),\"HaMvccTable1\",3)"}
		err = leaderPool.ExecuteTask(createTask)
		So(err, ShouldBeNil)

		time.Sleep(3 * time.Second)
		insertTask := &api.Task{Script: "tableInsert{loadHaMvccTable('HaMvccTable1')}", Args: values}
		err = followerPool.ExecuteTask(insertTask)
		So(err, ShouldBeNil)

		checkTask := &api.Task{Script: "each(eqObj, (select * from loadHaMvccTable('HaMvccTable1')).values(), table(1..100 as intv,take(`qq`ee`rr,100) as symbolv).values()).all()"}
		err = followerPool.ExecuteTask(checkTask)
		So(err, ShouldBeNil)
		So(checkTask.GetResult().(*model.Scalar).Value().(bool), ShouldBeTrue)
	})
}

func TestDBConnectionPool_tableInsert_haStreamTable_leader(t *testing.T) {
	Convey("TestDBConnectionPool_tableInsert_haStreamTable_leader", t, func() {
		conn, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)
		defer conn.Close()

		leaderRes, err := conn.RunScript(" exec port from rpc(getControllerAlias(), getClusterPerf) where name=getStreamingLeader(11);\n")
		So(err, ShouldBeNil)
		leaderPort := int(leaderRes.(*model.Vector).Get(0).Value().(int32))

		pool, err := api.NewDBConnectionPool(&api.PoolOption{
			Address:                setup.IP + ":" + strconv.Itoa(leaderPort),
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               1,
			EnableHighAvailability: true,
			HighAvailabilitySites:  setup.HA_sites,
		})
		So(err, ShouldBeNil)
		So(pool, ShouldNotBeNil)
		defer pool.Close()

		tmp, err := conn.RunScript("table(1..100 as intv,take(`qq`ee`rr,100) as symbolv)")
		So(err, ShouldBeNil)
		So(tmp, ShouldNotBeNil)

		values := []model.DataForm{tmp}
		createTask := &api.Task{Script: "try{dropStreamTable(\"haStreamTable1\")}catch(ex){};\n go;\n haStreamTable(11, table(array(INT) as intv,array(SYMBOL) as symbolv),\"haStreamTable1\",100000)"}
		err = pool.ExecuteTask(createTask)
		So(err, ShouldBeNil)

		time.Sleep(3 * time.Second)
		insertTask := &api.Task{Script: "tableInsert{haStreamTable1}", Args: values}
		err = pool.ExecuteTask(insertTask)
		So(err, ShouldBeNil)

		checkTask := &api.Task{Script: "each(eqObj, (select * from haStreamTable1).values(), table(1..100 as intv,take(`qq`ee`rr,100) as symbolv).values()).all()"}
		err = pool.ExecuteTask(checkTask)
		So(err, ShouldBeNil)
		So(checkTask.GetResult().(*model.Scalar).Value().(bool), ShouldBeTrue)
	})
}

func TestDBConnectionPool_tableInsert_haStreamTable_follower(t *testing.T) {
	Convey("TestDBConnectionPool_tableInsert_haStreamTable_follower", t, func() {
		conn, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)
		defer conn.Close()

		leaderRes, err := conn.RunScript(" exec port from rpc(getControllerAlias(), getClusterPerf) where name=getStreamingLeader(11);\n")
		So(err, ShouldBeNil)
		leaderPort := int(leaderRes.(*model.Vector).Get(0).Value().(int32))

		followerRes, err := conn.RunScript("tmp1=(exec sites[0] from getStreamingRaftGroups() where raftGroupName==\"11\").split(\",\");\ntmp2=each(x->split(x, \":\")[2],tmp1);\nexec port from rpc(getControllerAlias(), getClusterPerf) where name in tmp2  and name!=getStreamingLeader(11) limit 1;\n")
		So(err, ShouldBeNil)
		followerPort := int(followerRes.(*model.Vector).Get(0).Value().(int32))

		leaderPool, err := api.NewDBConnectionPool(&api.PoolOption{
			Address:                setup.IP + ":" + strconv.Itoa(leaderPort),
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               1,
			EnableHighAvailability: true,
			HighAvailabilitySites:  setup.HA_sites,
		})
		So(err, ShouldBeNil)
		So(leaderPool, ShouldNotBeNil)
		defer leaderPool.Close()
		fmt.Println("--------------------1111111111111------------------------------------")
		followerPool, err := api.NewDBConnectionPool(&api.PoolOption{
			Address:                setup.IP + ":" + strconv.Itoa(followerPort),
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               10,
			EnableHighAvailability: true,
			HighAvailabilitySites:  setup.HA_sites,
		})
		So(followerPool.GetPoolSize(), ShouldEqual, 10)
		fmt.Println("-------------------------222222222222-------------------------------")
		So(err, ShouldBeNil)
		So(followerPool, ShouldNotBeNil)
		defer followerPool.Close()

		tmp, err := conn.RunScript("table(1..100 as intv,take(`qq`ee`rr,100) as symbolv)")
		So(err, ShouldBeNil)
		So(tmp, ShouldNotBeNil)

		values := []model.DataForm{tmp}
		createTask := &api.Task{Script: "try{dropStreamTable(\"haStreamTable1\")}catch(ex){};\n go;\n haStreamTable(11, table(array(INT) as intv,array(SYMBOL) as symbolv),\"haStreamTable1\",100000)"}
		err = leaderPool.ExecuteTask(createTask)
		So(err, ShouldBeNil)

		time.Sleep(3 * time.Second)
		fmt.Println("--------------------------tableInsert之前------------------------------")
		insertTask := &api.Task{Script: "tableInsert{haStreamTable1}", Args: values}
		err = followerPool.ExecuteTask(insertTask)
		fmt.Println("--------------------------tableInsert之后------------------------------")
		So(followerPool.GetPoolSize(), ShouldEqual, 10)
		So(err, ShouldBeNil)
		time.Sleep(10 * time.Second)

		checkTask := &api.Task{Script: "each(eqObj, (select * from haStreamTable1).values(), table(1..100 as intv,take(`qq`ee`rr,100) as symbolv).values()).all()"}
		err = followerPool.ExecuteTask(checkTask)
		So(err, ShouldBeNil)
		So(checkTask.GetResult().(*model.Scalar).Value().(bool), ShouldBeTrue)
		fmt.Println("--------------------------tableInsert之前1111111------------------------------")
		insertTask1 := &api.Task{Script: "tableInsert{haStreamTable1}", Args: values}
		err = followerPool.ExecuteTask(insertTask1)
		fmt.Println("--------------------------tableInsert之前22222------------------------------")
		time.Sleep(3 * time.Second)
		checkTask1 := &api.Task{Script: "exec count(*) from haStreamTable1"}
		err = followerPool.ExecuteTask(checkTask1)

		So(err, ShouldBeNil)
		So(checkTask1.GetResult().(*model.Scalar).Value().(int32), ShouldEqual, 200)

		followerPool.Close()
		So(followerPool.GetPoolSize(), ShouldEqual, 0)
	})

	Convey("TestDBConnectionPool_tableInsert_haStreamTable_follower1", t, func() {
		conn, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)
		defer conn.Close()

		leaderRes, err := conn.RunScript(" exec port from rpc(getControllerAlias(), getClusterPerf) where name=getStreamingLeader(11);\n")
		So(err, ShouldBeNil)
		leaderPort := int(leaderRes.(*model.Vector).Get(0).Value().(int32))

		followerRes, err := conn.RunScript("tmp1=(exec sites[0] from getStreamingRaftGroups() where raftGroupName==\"11\").split(\",\");\ntmp2=each(x->split(x, \":\")[2],tmp1);\nexec port from rpc(getControllerAlias(), getClusterPerf) where name in tmp2  and name!=getStreamingLeader(11) limit 1;\n")
		So(err, ShouldBeNil)
		followerPort := int(followerRes.(*model.Vector).Get(0).Value().(int32))

		leaderPool, err := api.NewDBConnectionPool(&api.PoolOption{
			Address:                setup.IP + ":" + strconv.Itoa(leaderPort),
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               1,
			EnableHighAvailability: true,
			HighAvailabilitySites:  setup.HA_sites,
		})
		So(err, ShouldBeNil)
		So(leaderPool, ShouldNotBeNil)
		defer leaderPool.Close()
		fmt.Println("--------------------1111111111111------------------------------------")
		followerPool, err := api.NewDBConnectionPool(&api.PoolOption{
			Address:                setup.IP + ":" + strconv.Itoa(followerPort),
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               10,
			EnableHighAvailability: true,
			HighAvailabilitySites:  setup.HA_sites,
		})

		followerPool1, err := api.NewDBConnectionPool(&api.PoolOption{
			Address:                setup.IP + ":" + strconv.Itoa(followerPort),
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               5,
			EnableHighAvailability: true,
			HighAvailabilitySites:  setup.HA_sites,
		})
		fmt.Println("followerPool.GetPoolSize():", followerPool1.GetPoolSize())
		fmt.Println("-------------------------222222222222-------------------------------")
		So(err, ShouldBeNil)
		So(followerPool1, ShouldNotBeNil)
		defer followerPool1.Close()

		tmp, err := conn.RunScript("table(1..100 as intv,take(`qq`ee`rr,100) as symbolv)")
		So(err, ShouldBeNil)
		So(tmp, ShouldNotBeNil)

		values := []model.DataForm{tmp}
		createTask := &api.Task{Script: "try{dropStreamTable(\"haStreamTable1\")}catch(ex){};\n go;\n haStreamTable(11, table(array(INT) as intv,array(SYMBOL) as symbolv),\"haStreamTable1\",100000)"}
		err = leaderPool.ExecuteTask(createTask)
		So(err, ShouldBeNil)

		time.Sleep(3 * time.Second)
		fmt.Println("--------------------------tableInsert之前------------------------------")
		insertTask := &api.Task{Script: "tableInsert{haStreamTable1}", Args: values}
		err = followerPool.ExecuteTask(insertTask)
		fmt.Println("--------------------------tableInsert之后------------------------------")
		fmt.Println("followerPool.GetPoolSize():", followerPool.GetPoolSize())
		So(err, ShouldBeNil)
		time.Sleep(3 * time.Second)

		checkTask := &api.Task{Script: "each(eqObj, (select * from haStreamTable1).values(), table(1..100 as intv,take(`qq`ee`rr,100) as symbolv).values()).all()"}
		err = followerPool.ExecuteTask(checkTask)
		So(err, ShouldBeNil)
		So(checkTask.GetResult().(*model.Scalar).Value().(bool), ShouldBeTrue)
		fmt.Println("--------------------------tableInsert之前1111111------------------------------")
		insertTask1 := &api.Task{Script: "tableInsert{haStreamTable1}", Args: values}
		err = followerPool1.ExecuteTask(insertTask1)
		fmt.Println("--------------------------tableInsert之后22222------------------------------")
		time.Sleep(3 * time.Second)

		checkTask1 := &api.Task{Script: "exec count(*) from haStreamTable1"}
		err = followerPool1.ExecuteTask(checkTask1)
		So(err, ShouldBeNil)
		So(checkTask1.GetResult().(*model.Scalar).Value().(int32), ShouldEqual, 200)
	})

	Convey("TestDBConnectionPool_tableInsert_haStreamTable_follower2", t, func() {
		conn, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)
		defer conn.Close()

		leaderRes, err := conn.RunScript(" exec port from rpc(getControllerAlias(), getClusterPerf) where name=getStreamingLeader(11);\n")
		So(err, ShouldBeNil)
		leaderPort := int(leaderRes.(*model.Vector).Get(0).Value().(int32))

		followerRes, err := conn.RunScript("tmp1=(exec sites[0] from getStreamingRaftGroups() where raftGroupName==\"11\").split(\",\");\ntmp2=each(x->split(x, \":\")[2],tmp1);\nexec port from rpc(getControllerAlias(), getClusterPerf) where name in tmp2  and name!=getStreamingLeader(11) limit 1;\n")
		So(err, ShouldBeNil)
		followerPort := int(followerRes.(*model.Vector).Get(0).Value().(int32))

		leaderPool, err := api.NewDBConnectionPool(&api.PoolOption{
			Address:                setup.IP + ":" + strconv.Itoa(leaderPort),
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               1,
			EnableHighAvailability: true,
			HighAvailabilitySites:  setup.HA_sites,
		})
		So(err, ShouldBeNil)
		So(leaderPool, ShouldNotBeNil)
		defer leaderPool.Close()
		fmt.Println("--------------------1111111111111------------------------------------")
		followerPool, err := api.NewDBConnectionPool(&api.PoolOption{
			Address:                setup.IP + ":" + strconv.Itoa(followerPort),
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               10,
			EnableHighAvailability: true,
			HighAvailabilitySites:  setup.HA_sites,
		})

		followerPool1, err := api.NewDBConnectionPool(&api.PoolOption{
			Address:                setup.IP + ":" + strconv.Itoa(followerPort),
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               5,
			EnableHighAvailability: true,
			HighAvailabilitySites:  setup.HA_sites,
		})
		fmt.Println("followerPool.GetPoolSize():", followerPool1.GetPoolSize())
		fmt.Println("-------------------------222222222222-------------------------------")
		So(err, ShouldBeNil)
		So(followerPool1, ShouldNotBeNil)
		defer followerPool1.Close()

		tmp, err := conn.RunScript("table(1..100 as intv,take(`qq`ee`rr,100) as symbolv)")
		So(err, ShouldBeNil)
		So(tmp, ShouldNotBeNil)

		values := []model.DataForm{tmp}
		createTask := &api.Task{Script: "try{dropStreamTable(\"haStreamTable1\")}catch(ex){};\n go;\n haStreamTable(11, table(array(INT) as intv,array(SYMBOL) as symbolv),\"haStreamTable1\",100000)"}
		err = leaderPool.ExecuteTask(createTask)
		So(err, ShouldBeNil)

		time.Sleep(3 * time.Second)
		fmt.Println("--------------------------tableInsert之前------------------------------")
		insertTask := &api.Task{Script: "tableInsert{haStreamTable1}", Args: values}
		err = followerPool.ExecuteTask(insertTask)
		fmt.Println("--------------------------tableInsert之后------------------------------")
		fmt.Println("followerPool.GetPoolSize():", followerPool.GetPoolSize())
		So(err, ShouldBeNil)
		time.Sleep(3 * time.Second)

		checkTask := &api.Task{Script: "each(eqObj, (select * from haStreamTable1).values(), table(1..100 as intv,take(`qq`ee`rr,100) as symbolv).values()).all()"}
		err = followerPool.ExecuteTask(checkTask)
		So(err, ShouldBeNil)
		So(checkTask.GetResult().(*model.Scalar).Value().(bool), ShouldBeTrue)

		insertTask1 := &api.Task{Script: "tableInsert{haStreamTable1}", Args: values}
		err = followerPool.ExecuteTask(insertTask1)

		time.Sleep(3 * time.Second)
		fmt.Println("--------------------------tableInsert之前1111111------------------------------")
		checkTask1 := &api.Task{Script: "exec count(*) from haStreamTable1"}
		err = followerPool1.ExecuteTask(checkTask1)
		fmt.Println("--------------------------tableInsert之后22222------------------------------")
		So(err, ShouldBeNil)
		So(checkTask1.GetResult().(*model.Scalar).Value().(int32), ShouldEqual, 200)
	})
}
