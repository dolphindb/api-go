package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dolphindb/api-go/v3/api"
	"github.com/dolphindb/api-go/v3/model"
	"github.com/dolphindb/api-go/v3/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

func TestConnnectionPoolOption_NetTimeout1(t *testing.T) {
	SkipConvey("TestConnnectionPoolOption_NetTimeout_Reconnect_true", t, func() {
		opt := &api.PoolOption{
			Address:    setup.Address,
			UserID:     setup.UserName,
			Password:   setup.Password,
			PoolSize:   10,
			NetTimeout: 11118 * time.Second,
			Reconnect:  true,
		}
		//server所在机器网络断开 检查重连超时时间
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		So(pool, ShouldNotBeNil)
	})

	SkipConvey("TestConnnectionPoolOption_NetTimeout_EnableHighAvailability_true", t, func() {
		opt := &api.PoolOption{
			Address:                setup.Address,
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               10,
			NetTimeout:             8 * time.Second,
			Reconnect:              true,
			EnableHighAvailability: true,
			HighAvailabilitySites:  []string{setup.Address2, setup.Address3, setup.Address4},
		}
		//server所在机器网络断开 检查重连超时时间
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		So(pool, ShouldNotBeNil)
	})

	SkipConvey("TestConnnectionPoolOption_NetTimeout_EnableHighAvailability_true_LoadBalance_true", t, func() {
		reconnNum := 3
		opt := &api.PoolOption{
			Address:                setup.Address,
			UserID:                 setup.UserName,
			Password:               setup.Password,
			PoolSize:               10,
			NetTimeout:             5 * time.Second,
			TryReconnectNums:       &reconnNum,
			Reconnect:              true,
			LoadBalance:            true,
			LoadBalanceAddresses:   []string{setup.Address2, setup.Address3, setup.Address4},
			EnableHighAvailability: true,
			HighAvailabilitySites:  []string{setup.Address2, setup.Address3, setup.Address4},
		}
		//server所在机器网络断开 检查重连超时时间
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		So(pool, ShouldNotBeNil)
	})

	SkipConvey("TestConnnectionPoolOption_execute_NetTimeout_Reconnect_true", t, func() {
		opt := &api.PoolOption{
			Address:    setup.Address,
			UserID:     setup.UserName,
			Password:   setup.Password,
			PoolSize:   10,
			NetTimeout: 8 * time.Second,
			Reconnect:  true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		So(pool, ShouldNotBeNil)
		fmt.Println("pool created, now disconnect the network of server, and check if the pool can reconnect within 8 seconds")
		//server所在机器网络断开 检查重连超时时间
		for {
			execTask := &api.Task{Script: "a=1;\n a"}
			err = pool.Execute([]*api.Task{execTask})
			So(err, ShouldBeNil)
			So(execTask.GetError(), ShouldBeNil)
			So(execTask.IsSuccess(), ShouldBeTrue)
			So(execTask.GetResult(), ShouldNotBeNil)
			fmt.Println(execTask.GetResult(), "execute successfully, now check if the connection is reconnected")
			time.Sleep(3 * time.Second)
		}
	})
	//应用层
	SkipConvey("TestConnnectionPoolOption_NetTimeout_Reconnect_true_idle_time", t, func() {
		opt := &api.PoolOption{
			Address:    setup.Address,
			UserID:     setup.UserName,
			Password:   setup.Password,
			PoolSize:   10,
			NetTimeout: 8 * time.Second,
			Reconnect:  true,
		}
		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		So(pool, ShouldNotBeNil)
		fmt.Println("pool created, now disconnect the network of server, and check if the pool can reconnect within 8 seconds")
		for {
			fmt.Println("idle time, now check if the connection is reconnected")
			time.Sleep(3 * time.Second)
			//server所在机器网络断开 检查重连超时时间
		}
	})
}

func TestDBConnectionPool_Address_disconnection(t *testing.T) {
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
	SkipConvey("TestDBConnectionPool_Address_disconnection_EnableHighAvailability_true_TryReconnectNums_10", t, func() {
		//
		time.Sleep(3 * time.Second)
		reconnNum := 10
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
		//重连10次后不再重连
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

func TestDBConnectionPool_datanode_disconnection(t *testing.T) {
	SkipConvey("TestDBConnectionPool_datanode_disconnection", t, func() {
		time.Sleep(3 * time.Second)
		opt := &api.PoolOption{
			Address:  setup.Address,
			UserID:   setup.UserName,
			Password: setup.Password,
			PoolSize: 20,
			//Timeout:  time.Second * 2,
			//Reconnect:              true,
			//TryReconnectNums: &reconnNum,
			LoadBalance: true,
			//LoadBalanceAddresses: []string{setup.Address, setup.Address2, setup.Address3, setup.Address4},
			//EnableHighAvailability: true,
			HighAvailabilitySites: []string{setup.Address, setup.Address2, setup.Address3, setup.Address4},
		}

		pool, err := api.NewDBConnectionPool(opt)
		So(err, ShouldBeNil)
		So(pool, ShouldNotBeNil)
		So(pool.GetPoolSize(), ShouldEqual, 20)
		So(pool.IsClosed(), ShouldBeFalse)

		// for i := 0; i < 1000; i++ {
		//         execTask := &api.Task{Script: "select count(*) from loadTable('dfs://compo_value_range',`pt)"}
		//         err = pool.Execute([]*api.Task{execTask})
		//         time.Sleep(500 * time.Millisecond)
		//         fmt.Printf("query %d: err=%v, result=%v\n", i+1, err, execTask.GetResult())
		// }
	})

}
