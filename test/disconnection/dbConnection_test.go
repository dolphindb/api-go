package test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/dolphindb/api-go/v3/api"
	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/dolphindb/api-go/v3/logging"
	"github.com/dolphindb/api-go/v3/model"
	"github.com/dolphindb/api-go/v3/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

func TestBehaviorOptions_NetTimeout1(t *testing.T) {
	logging.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	Convey("Test_BehaviorOptions_NetTimeout_Reconnect_true", t, func() {
		opt := &dialer.BehaviorOptions{
			Reconnect:  true,
			NetTimeout: 100000 * time.Second,
		}
		//server所在机器网络断开 检查重连超时时间
		conn, _ := api.NewDolphinDBClient(context.TODO(), setup.Address, opt)
		err := conn.Connect()
		err1 := conn.ConnLogin("admin", "123456")
		So(err, ShouldBeNil)
		So(err1, ShouldBeNil)
	})

	SkipConvey("Test_BehaviorOptions_NetTimeout_Reconnect_true_EnableHighAvailability_true", t, func() {
		opt := &dialer.BehaviorOptions{
			Reconnect:              true,
			NetTimeout:             5 * time.Second,
			EnableHighAvailability: true,
			HighAvailabilitySites:  []string{setup.Address2, setup.Address3, setup.Address4},
		}
		//server所在机器网络断开 检查重连超时时间
		conn, _ := api.NewDolphinDBClient(context.TODO(), setup.Address, opt)
		err := conn.Connect()
		So(err, ShouldBeNil)
	})

	SkipConvey("Test_BehaviorOptions_NetTimeout_Reconnect_true_EnableHighAvailability_true_LoadBalance_true", t, func() {
		reconnNum := 3
		opt := &dialer.BehaviorOptions{
			Reconnect:              true,
			NetTimeout:             5 * time.Second,
			TryReconnectNums:       &reconnNum,
			EnableHighAvailability: true,
			LoadBalance:            true,
			HighAvailabilitySites:  []string{setup.Address2, setup.Address3, setup.Address4},
		}
		//server所在机器网络断开 检查重连超时时间
		conn, _ := api.NewDolphinDBClient(context.TODO(), setup.Address, opt)
		err := conn.Connect()
		So(err, ShouldBeNil)
	})

	SkipConvey("Test_BehaviorOptions_run_NetTimeout_Reconnect_true", t, func() {
		//reconnNum := 3
		opt := &dialer.BehaviorOptions{
			Reconnect:  true,
			NetTimeout: 5 * time.Second,
			//TryReconnectNums: &reconnNum,
			//EnableHighAvailability: true,
			//LoadBalance:            true,
			//HighAvailabilitySites:  []string{setup.Address2, setup.Address3, setup.Address4},
		}

		conn, _ := api.NewDolphinDBClient(context.TODO(), setup.Address, opt)
		err := conn.Connect()
		So(err, ShouldBeNil)
		//server所在机器网络断开 检查重连超时时间
		for {
			res, err := conn.RunScript("a=1;\n a")
			So(err, ShouldBeNil)
			So(res, ShouldNotBeNil)
			fmt.Println("script run result:", res.String())
			time.Sleep(3 * time.Second)
		}
	})
}

// AG-163 连接win和linux server 重复多次断开再重启 检查api连接状态
func TestConnectionRun(t *testing.T) {
	logging.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))
	SkipConvey("Test_BehaviorOptions_Reconnect_true", t, func() {

		//tryReconnectNums := 3
		opt := &dialer.BehaviorOptions{
			Reconnect:              true,
			EnableHighAvailability: true,
			HighAvailabilitySites:  []string{setup.Address2, setup.Address3, setup.Address4},
			LoadBalance:            false,
			//TryReconnectNums:       &tryReconnectNums,
		}
		// fmt.Println("datanode disconnected")
		conn, _ := api.NewDolphinDBClient(context.TODO(), setup.Address, opt)
		err1 := conn.Connect()
		So(err1, ShouldBeNil)
		for {
			conn.RunScript(`
			1+1`)
			fmt.Println("This is an infinite loop")
			time.Sleep(2 * time.Second) // 避免CPU跑满
		}
	})
}

func TestReceiveRepeatData_DisconnectionDFS(t *testing.T) {
	SkipConvey("TestReceiveRepeatData_Disconnection_dfs", t, func() {
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
		loginReq := &api.LoginRequest{
			UserID:   "admin",
			Password: "123456",
		}
		err = connection.Login(loginReq)

		_, err = connection.RunScript("dbPath = \"dfs://tableUpsert_test\"\nif(existsDatabase(dbPath)){\n\tdropDatabase(dbPath)\n}\nt = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\ndb = database(dbPath, RANGE, 1 50 10000)\npt = db.createPartitionedTable(t,`pt,`id).append!(t)")
		So(err, ShouldBeNil)

		tmp, err := connection.RunScript("table(take(1000,3000000) as id, 1..3000000 as id2, 1..3000000 as value)")
		So(err, ShouldBeNil)
		So(tmp, ShouldNotBeNil)

		values := []model.DataForm{tmp}
		fmt.Println("---------------------------------Read data end------------------------------------")
		time.Sleep(5 * time.Second)
		fmt.Println("Start Write!!!!!!!!!!!!!!!!!")
		for i := 0; i < 100; i++ {
			_, err = connection.RunFunc("tableInsert{loadTable(\"dfs://tableUpsert_test\",\"pt\")}", values)
			//So(err, ShouldBeNil)
			fmt.Println("数据插入", i, "次")
		}
		fmt.Println("End Write!!!!!!!!!!!!!!!!!!!")

		res, err := connection.RunScript("select count(*) from loadTable(\"dfs://tableUpsert_test\",\"pt\")")
		//So(err, ShouldBeNil)
		//So(res, ShouldNotBeNil)
		fmt.Println("The result is:\n", res.String())
	})
}

func TestReceiveRepeatData_DisconnectionHAStreamTable(t *testing.T) {
	logging.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))
	Convey("TestReceiveRepeatData_Disconnection_haTable", t, func() {
		opt := &dialer.BehaviorOptions{
			EnableHighAvailability: true,
			HighAvailabilitySites:  []string{setup.Address2, setup.Address3, setup.Address},
		}

		connection, err := api.NewDolphinDBClient(context.TODO(), setup.Address2, opt)
		So(err, ShouldBeNil)
		So(connection, ShouldNotBeNil)
		defer connection.Close()

		err = connection.Connect()
		So(err, ShouldBeNil)

		connCtl, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.CtlAdress, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		So(connCtl, ShouldNotBeNil)

		_, err = connection.RunScript("try{dropStreamTable(\"trades\")}catch(ex){}\nt = table(10:0,`id1`id2`id3,[INT,INT,INT])\nhaStreamTable(11,t,`trades,100000);")
		So(err, ShouldBeNil)

		tmp, err := connection.RunScript("table(take(1000,1000) as id, 1..1000 as id2, 1..1000 as value)")
		So(err, ShouldBeNil)
		So(tmp, ShouldNotBeNil)

		values := []model.DataForm{tmp}
		fmt.Println("---------------------------------Read data end------------------------------------")
		time.Sleep(5 * time.Second)
		fmt.Println("Start Write!!!!!!!!!!!!!!!!!")
		for i := 0; i < 10; i++ {
			_, err = connection.RunFunc("tableInsert{trades}", values)
			So(err, ShouldBeNil)
			fmt.Println("数据插入", i, "次")
			fmt.Println("-------------断网/节点断掉---------------")
			leader_node, _ := connCtl.RunScript("rpc((exec name from getClusterPerf() where mode=0 and state=1 limit 1)[0], getStreamingLeader,11)")
			fmt.Println("now", leader_node.(*model.Scalar).Value().(string), "is connected, try to stop it")
			connCtl.RunScript("stopDataNode(`" + leader_node.(*model.Scalar).Value().(string) + ")")
			time.Sleep(8 * time.Second)

			_, err = connection.RunFunc("tableInsert{trades}", values)
			connCtl.RunScript("startDataNode(`" + leader_node.(*model.Scalar).Value().(string) + ")")
			time.Sleep(8 * time.Second)
		}
		fmt.Println("End Write!!!!!!!!!!!!!!!!!!!")
		res, err := connection.RunScript("select count(*) from trades")
		So(err, ShouldBeNil)
		So(res, ShouldNotBeNil)
		fmt.Println("The result is:\n", res.String())
	})
}
