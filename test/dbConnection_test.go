package test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/dolphindb/api-go/v3/api"
	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/dolphindb/api-go/v3/model"
	"github.com/dolphindb/api-go/v3/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

var host3 = getRandomClusterAddress()

func TestNewDolphinDBClient(t *testing.T) {
	t.Parallel()
	Convey("func NewDolphinDB exception test", t, func() {
		Convey("Test NewDolphinDB wrong address exception", func() {
			_, err := api.NewDolphinDBClient(context.TODO(), "123456", nil)
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})

		Convey("Test NewDolphinDB login wrong userName exception", func() {
			db, _ := api.NewDolphinDBClient(context.TODO(), host3, nil)
			err := db.Connect()
			So(err, ShouldBeNil)
			defer db.Close()
			loginReq := new(api.LoginRequest).
				SetUserID("wrongName").
				SetPassword(setup.Password)
			err = db.Login(loginReq)
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})

		Convey("Test NewDolphinDB login wrong password exception", func() {
			db, _ := api.NewDolphinDBClient(context.TODO(), host3, nil)
			err := db.Connect()
			So(err, ShouldBeNil)
			defer db.Close()
			loginReq := new(api.LoginRequest).
				SetUserID(setup.UserName).
				SetPassword("wrong password")
			err = db.Login(loginReq)
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})
	})

	Convey("Test NewDolphinDB login and logout", t, func() {
		Convey("Test NewDolphinDB login", func() {
			db, err := api.NewDolphinDBClient(context.TODO(), host3, nil)
			So(err, ShouldBeNil)
			err = db.Connect()
			So(err, ShouldBeNil)
			defer db.Close()
			loginReq := new(api.LoginRequest).
				SetUserID(setup.UserName).
				SetPassword(setup.Password)
			err = db.Login(loginReq)
			So(err, ShouldBeNil)
		})

		Convey("Test NewDolphinDB logout", func() {
			db, _ := api.NewDolphinDBClient(context.TODO(), host3, nil)
			err := db.Connect()
			So(err, ShouldBeNil)
			defer db.Close()
			loginReq := new(api.LoginRequest).
				SetUserID(setup.UserName).
				SetPassword(setup.Password)
			err = db.Login(loginReq)
			So(err, ShouldBeNil)
			err = db.Logout()
			So(err, ShouldBeNil)
		})
	})
}

func TestNewSimpleDolphinDBClient(t *testing.T) {
	t.Parallel()
	Convey("func NewSimpleDolphinDB exception test", t, func() {
		Convey("Test NewSimpleDolphinDB wrong address exception", func() {
			_, err := api.NewSimpleDolphinDBClient(context.TODO(), "wrongAddress", setup.UserName, setup.Password)
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})

		Convey("Test NewSimpleDolphinDB wrong userName int exception", func() {
			_, err := api.NewSimpleDolphinDBClient(context.TODO(), host3, "1234", setup.Password)
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})

		Convey("Test NewSimpleDolphinDB wrong password exception", func() {
			_, err := api.NewSimpleDolphinDBClient(context.TODO(), host3, setup.UserName, "12")
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})
	})

	Convey("Test NewSimpleDolphinDB login and logout", t, func() {
		Convey("Test NewSimpleDolphinDB login", func() {
			db, err := api.NewSimpleDolphinDBClient(context.TODO(), host3, setup.UserName, setup.Password)
			So(err, ShouldBeNil)
			defer db.Close()
			dbName := `dfs://` + generateRandomString(10)
			re, err := db.RunScript(
				`dbName='` + dbName + `'
					if(existsDatabase(dbName)){
						dropDatabase(dbName)
					}
					db=database(dbName, VALUE, 1..10)
					db`)
			So(err, ShouldBeNil)
			s := re.(*model.Scalar)
			result := s.DataType.Value()
			ex := "DB[" + dbName + "]"
			So(result, ShouldEqual, ex)
			db.DropDatabase(&api.DropDatabaseRequest{Directory: dbName})
		})

		Convey("Test NewSimpleDolphinDB logout", func() {
			db, _ := api.NewSimpleDolphinDBClient(context.TODO(), host3, setup.UserName, setup.Password)
			err := db.Logout()
			So(err, ShouldBeNil)
			defer db.Close()
			dbName := `dfs://` + generateRandomString(10)
			re, err := db.RunScript(`
			dbName="` + dbName + `"
			if(existsDatabase(dbName)){
				dropDatabase(dbName)
			}
			db=database(dbName, VALUE, 1..10)`)
			result := fmt.Errorf("\n error is %w", err)
			So(re, ShouldBeNil)
			So(result, ShouldNotBeNil)
			db.DropDatabase(&api.DropDatabaseRequest{Directory: dbName})
		})
	})
}

func TestClose(t *testing.T) {
	t.Parallel()
	Convey("Test connection Close", t, func() {
		Convey("Test NewDolphinDB Close", func() {
			db, err := api.NewDolphinDBClient(context.TODO(), host3, nil)
			So(err, ShouldBeNil)
			err = db.Connect()
			So(err, ShouldBeNil)
			db.Close()
			connections, err := db.RunScript("getConnections()")
			So(connections, ShouldBeNil)
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})

		Convey("Test NewSimpleDolphinDB Close", func() {
			db, err := api.NewSimpleDolphinDBClient(context.TODO(), host3, setup.UserName, setup.Password)
			So(err, ShouldBeNil)
			db.Close()
			connections, err := db.RunScript("getConnections()")
			So(connections, ShouldBeNil)
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})
	})
}

func TestIsClosed(t *testing.T) {
	t.Parallel()
	Convey("Test connection IsClosed", t, func() {
		Convey("Test NewDolphinDB IsClosed", func() {
			db, err := api.NewDolphinDBClient(context.TODO(), host3, nil)
			So(err, ShouldBeNil)
			err = db.Connect()
			So(err, ShouldBeNil)
			IsClosedd := db.IsClosed()
			So(IsClosedd, ShouldEqual, false)
			err = db.Close()
			IsClosedd = db.IsClosed()
			So(err, ShouldBeNil)
			So(IsClosedd, ShouldEqual, true)
		})

		Convey("Test NewSimpleDolphinDB IsClosed", func() {
			db, err := api.NewSimpleDolphinDBClient(context.TODO(), host3, setup.UserName, setup.Password)
			So(err, ShouldBeNil)
			IsClosedd := db.IsClosed()
			So(IsClosedd, ShouldEqual, false)
			err = db.Close()
			IsClosedd = db.IsClosed()
			So(err, ShouldBeNil)
			So(IsClosedd, ShouldEqual, true)
		})
	})
}

func TestRefreshTimeout(t *testing.T) {
	t.Parallel()
	Convey("Test RefreshTimeout NewSimpleConn", t, func() {
		db, err := dialer.NewSimpleConn(context.TODO(), host3, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		SessionID1 := db.GetSession()
		So(SessionID1, ShouldNotBeNil)
		db.RefreshTimeout(100)
		SessionID2 := db.GetSession()
		So(SessionID1, ShouldEqual, SessionID2)
		db.Close()
	})
	Convey("Test RefreshTimeout NewConn", t, func() {
		db, err := dialer.NewConn(context.TODO(), host3, nil)
		So(err, ShouldBeNil)
		err = db.Connect()
		So(err, ShouldBeNil)
		SessionID1 := db.GetSession()
		So(SessionID1, ShouldNotBeNil)
		db.RefreshTimeout(100)
		SessionID2 := db.GetSession()
		So(SessionID1, ShouldEqual, SessionID2)
		db.Close()
	})
}
func TestGetSession(t *testing.T) {
	t.Parallel()
	Convey("Test connection GetSession", t, func() {
		Convey("Test NewDolphinDB GetSession", func() {
			db, err := api.NewDolphinDBClient(context.TODO(), host3, nil)
			So(err, ShouldBeNil)
			err = db.Connect()
			So(err, ShouldBeNil)
			SessionID := db.GetSession()
			So(SessionID, ShouldNotBeNil)
			err = db.Close()
			SessionID = db.GetSession()
			So(err, ShouldBeNil)
			So(SessionID, ShouldEqual, "")
		})

		Convey("Test NewSimpleDolphinDB GetSession", func() {
			db, err := api.NewSimpleDolphinDBClient(context.TODO(), host3, setup.UserName, setup.Password)
			So(err, ShouldBeNil)
			SessionID := db.GetSession()
			So(SessionID, ShouldNotBeNil)
			err = db.Close()
			SessionID = db.GetSession()
			So(err, ShouldBeNil)
			So(SessionID, ShouldEqual, "")
		})
	})
}

func TestNewConn(t *testing.T) {
	t.Parallel()
	Convey("func NewConn exception test", t, func() {
		Convey("Test NewConn wrong address exception", func() {
			_, err := dialer.NewConn(context.TODO(), "123456", nil)
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})
	})
	Convey("Test NewConn connection", t, func() {
		db, err := dialer.NewConn(context.TODO(), host3, nil)
		So(err, ShouldBeNil)
		err = db.Connect()
		So(err, ShouldBeNil)
		SessionID := db.GetSession()
		So(SessionID, ShouldNotBeNil)
		err = db.Close()
		So(err, ShouldBeNil)
	})
}

func TestNewSimpleConn(t *testing.T) {
	t.Parallel()
	Convey("func NewSimpleConn exception test", t, func() {
		Convey("Test NewSimpleConn wrong address exception", func() {
			_, err := dialer.NewSimpleConn(context.TODO(), "wrongAddress", setup.UserName, setup.Password)
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})

		Convey("Test NewSimpleConn wrong userName int exception", func() {
			_, err := dialer.NewSimpleConn(context.TODO(), host3, "1234", setup.Password)
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})

		Convey("Test NewSimpleConn wrong password exception", func() {
			_, err := dialer.NewSimpleConn(context.TODO(), host3, setup.UserName, "12")
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})
	})

	Convey("Test NewSimpleConn login and logout", t, func() {
		Convey("Test NewSimpleConn login", func() {
			db, err := dialer.NewSimpleConn(context.TODO(), host3, setup.UserName, setup.Password)
			So(err, ShouldBeNil)
			dbName := `"dfs://test"`
			re, err := db.RunScript(
				`dbName=` + dbName + `
					if(existsDatabase(dbName)){
						dropDatabase(dbName)
					}
					db=database(dbName, VALUE, 1..10)
					db`)
			So(err, ShouldBeNil)
			s := re.(*model.Scalar)
			result := s.DataType.Value()
			ex := "DB[dfs://test]"
			So(result, ShouldEqual, ex)
			db.Close()
		})

		Convey("Test NewSimpleConn getSessionId", func() {
			db, _ := dialer.NewSimpleConn(context.TODO(), host3, setup.UserName, setup.Password)
			re, err := db.RunScript(`
			dbName="dfs://test"
			if(existsDatabase(dbName)){
				dropDatabase(dbName)
			}
			db=database(dbName, VALUE, 1..10)`)
			result := fmt.Errorf("\n error is %w", err)
			So(re, ShouldBeNil)
			So(result, ShouldNotBeNil)
			SessionID := db.GetSession()
			So(SessionID, ShouldNotBeNil)
			add := db.GetLocalAddress()
			So(add, ShouldEqual, setup.LocalIP)
			err = db.Close()
			So(err, ShouldBeNil)
		})
	})
}

func TestGetLocalAddress(t *testing.T) {
	t.Parallel()
	Convey("Test GetLocalAddress NewSimpleConn", t, func() {
		db, err := dialer.NewSimpleConn(context.TODO(), host3, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		So(db.IsConnected(), ShouldBeTrue)
		re := db.GetLocalAddress()
		So(re, ShouldEqual, setup.LocalIP)
		db.Close()
	})
	Convey("Test GetLocalAddress NewConn", t, func() {
		db, err := dialer.NewConn(context.TODO(), host3, nil)
		db.Connect()
		So(err, ShouldBeNil)
		re := db.GetLocalAddress()
		So(re, ShouldEqual, setup.LocalIP)
		db.Close()
	})
	Convey("Test GetLocalAddress NewSimpleDolphinDBClient", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), host3, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		re := db.GetLocalAddress()
		So(re, ShouldEqual, setup.LocalIP)
		db.Close()
	})
	Convey("Test GetLocalAddress NewDolphinDBClient", t, func() {
		db, err := api.NewDolphinDBClient(context.TODO(), host3, nil)
		So(err, ShouldBeNil)
		err = db.Connect()
		So(err, ShouldBeNil)
		re := db.GetLocalAddress()
		So(re, ShouldEqual, setup.LocalIP)
		db.Close()
	})
}

func TestConnectionHighAvailability(t *testing.T) {
	t.SkipNow()
	SkipConvey("TestConnectionHighAvailability", t, func() {
		opt := &dialer.BehaviorOptions{
			EnableHighAvailability: true,
			HighAvailabilitySites:  setup.HA_sites,
		}
		connHA, err := api.NewDolphinDBClient(context.TODO(), setup.Address4, opt)
		AssertNil(err)
		connCtl, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.CtlAdress, setup.UserName, setup.Password)
		AssertNil(err)
		err = connHA.Connect()
		AssertNil(err)
		loginReq := &api.LoginRequest{
			UserID:   setup.UserName,
			Password: setup.Password,
		}
		err = connHA.Login(loginReq)
		AssertNil(err)
		origin_node, _ := connHA.RunScript("getNodeAlias()")
		fmt.Println("now", origin_node.(*model.Scalar).Value().(string), "is connected, try to stop it")
		connCtl.RunScript("stopDataNode(`" + origin_node.(*model.Scalar).Value().(string) + ")")
		time.Sleep(2 * time.Second)
		fmt.Println("stop success, check if the origin connection click to another node")
		res, err := connHA.RunScript("getNodeAlias()")
		AssertNil(err)
		So(res.String(), ShouldNotEqual, origin_node.(*model.Scalar).Value().(string))
		fmt.Println("check passed, restart the origin node")
		_, err = connCtl.RunScript(
			"nodes = exec name from getClusterPerf() where state!=1 and mode !=1;" +
				"startDataNode(nodes);")
		AssertNil(err)
		time.Sleep(2 * time.Second)
		connCtl.Close()
		connHA.Close()
		So(connHA.IsClosed(), ShouldBeTrue)
	})
	Convey("TestConnnectionHighAvailability exception", t, func() {
		opt := &dialer.BehaviorOptions{
			EnableHighAvailability: true,
			// HighAvailabilitySites:  setup.HA_sites,
		}
		_, err := api.NewDolphinDBClient(context.TODO(), setup.Address4, opt)
		So(err.Error(), ShouldContainSubstring, "if EnableHighAvailability is true, HighAvailabilitySites should be specified")

		// opt = &dialer.BehaviorOptions{
		//         // EnableHighAvailability: true,
		//         HighAvailabilitySites: setup.HA_sites,
		// }
		// _, err = api.NewDolphinDBClient(context.TODO(), setup.Address4, opt)
		// So(err.Error(), ShouldContainSubstring, "connect to all sites failed")

		// opt = &dialer.BehaviorOptions{
		//         EnableHighAvailability: false,
		//         HighAvailabilitySites:  setup.HA_sites,
		// }
		// _, err = api.NewDolphinDBClient(context.TODO(), setup.Address4, opt)
		// So(err.Error(), ShouldContainSubstring, "connect to all sites failed")
	})
	timeout := time.Second
	Convey("Test_BehaviorOptions_Reconnect_true", t, func() {
		reconnNum := 1
		opt := &dialer.BehaviorOptions{
			Reconnect:        true,
			TryReconnectNums: &reconnNum,
			Timeout:          timeout,
		}
		connCtl, _ := api.NewSimpleDolphinDBClient(context.TODO(), setup.CtlAdress, setup.UserName, setup.Password)
		So(connCtl.IsConnected(), ShouldBeTrue)
		conn, err := api.NewDolphinDBClient(context.TODO(), host3, opt)
		So(err, ShouldBeNil)
		conn.Connect()
		loginReq := &api.LoginRequest{
			UserID:   "admin",
			Password: "123456",
		}
		err = conn.Login(loginReq)
		So(err, ShouldBeNil)
		nodeName, _ := conn.RunScript("getNodeAlias()")
		connCtl.RunScript("stopDataNode(`" + nodeName.(*model.Scalar).Value().(string) + "); assert not (exec state from getClusterPerf() where name=`" + nodeName.(*model.Scalar).Value().(string) + ")")
		fmt.Println(nodeName.(*model.Scalar).Value().(string) + " stopped successfully")
		time.Sleep(2 * time.Second)
		connCtl.RunScript("startDataNode(`" + nodeName.(*model.Scalar).Value().(string) + "); assert (exec state from getClusterPerf() where name=`" + nodeName.(*model.Scalar).Value().(string) + ")")
		fmt.Println(nodeName.(*model.Scalar).Value().(string) + " started successfully")
		time.Sleep(2 * time.Second)
		res, _ := conn.RunScript("1+1")
		So(res.(*model.Scalar).Value().(int32), ShouldEqual, 2)
		conn.Close()
		connCtl.Close()
	})

	Convey("Test_BehaviorOptions_Reconnect_true_reconnNum", t, func() {
		reconnNum := 1
		opt := &dialer.BehaviorOptions{
			Reconnect:        true,
			TryReconnectNums: &reconnNum,
			Timeout:          timeout,
		}
		connCtl, _ := api.NewSimpleDolphinDBClient(context.TODO(), setup.CtlAdress, setup.UserName, setup.Password)
		So(connCtl.IsConnected(), ShouldBeTrue)
		conn, err := api.NewDolphinDBClient(context.TODO(), host3, opt)
		So(err, ShouldBeNil)
		conn.Connect()
		loginReq := &api.LoginRequest{
			UserID:   "admin",
			Password: "123456",
		}
		err = conn.Login(loginReq)
		So(err, ShouldBeNil)
		nodeName, _ := conn.RunScript("getNodeAlias()")
		connCtl.RunScript("stopDataNode(`" + nodeName.(*model.Scalar).Value().(string) + "); assert not (exec state from getClusterPerf() where name=`" + nodeName.(*model.Scalar).Value().(string) + ")")
		fmt.Println(nodeName.(*model.Scalar).Value().(string) + " stopped successfully")
		go func() {
			res, err := conn.RunScript("1+1")
			Convey("Test_BehaviorOptions_Reconnect_true_reconnNum_res", t, func() {
				So(err, ShouldNotBeNil)
				So(res, ShouldBeNil)
			})
		}()
		time.Sleep(5 * time.Second)
		connCtl.RunScript("startDataNode(`" + nodeName.(*model.Scalar).Value().(string) + "); assert (exec state from getClusterPerf() where name=`" + nodeName.(*model.Scalar).Value().(string) + ")")
		fmt.Println(nodeName.(*model.Scalar).Value().(string) + " started successfully")
		time.Sleep(2 * time.Second)
		conn.Close()
		connCtl.Close()
	})
}

func TestConnectionParallel(t *testing.T) {
	db, err := api.NewSimpleDolphinDBClient(context.TODO(), host3, "admin", "123456")
	AssertNil(err)
	db.RunScript("login(`admin,`123456);try{createUser(`test1, `123456)}catch(ex){};go;setMaxJobParallelism(`test1, 10);setMaxJobPriority(`test1, 6);")
	Convey("TestConnectionParallel_lt_MaxJobParallelism", t, func() {

		priority := 0
		parallel := 1
		opt := &dialer.BehaviorOptions{
			Priority:    &priority,
			Parallelism: &parallel,
		}
		conn, err := api.NewDolphinDBClient(context.TODO(), host3, opt)
		So(err, ShouldBeNil)
		conn.Connect()
		loginReq := &api.LoginRequest{
			UserID:   "test1",
			Password: "123456",
		}
		err = conn.Login(loginReq)
		So(err, ShouldBeNil)
		res, _ := conn.RunScript("getConsoleJobs()")
		Println(res)
		So(res.(*model.Table).GetColumnByName("parallelism").Get(0).Value().(int32), ShouldEqual, 1)
		So(res.(*model.Table).GetColumnByName("priority").Get(0).Value().(int32), ShouldEqual, 0)

		conn.Close()
		So(conn.IsClosed(), ShouldBeTrue)
	})

	Convey("TestConnectionParallel_gt_MaxJobParallelism", t, func() {

		priority := 8
		parallel := 11
		opt := &dialer.BehaviorOptions{
			Priority:    &priority,
			Parallelism: &parallel,
		}
		conn, err := api.NewDolphinDBClient(context.TODO(), host3, opt)
		So(err, ShouldBeNil)
		conn.Connect()
		loginReq := &api.LoginRequest{
			UserID:   "test1",
			Password: "123456",
		}
		err = conn.Login(loginReq)
		So(err, ShouldBeNil)
		res, _ := conn.RunScript("getConsoleJobs()")
		Println(res)
		So(res.(*model.Table).GetColumnByName("parallelism").Get(0).Value().(int32), ShouldEqual, 10)
		So(res.(*model.Table).GetColumnByName("priority").Get(0).Value().(int32), ShouldEqual, 6)

		conn.Close()
		So(conn.IsClosed(), ShouldBeTrue)
	})

	Convey("TestConnectionParallel_default", t, func() {
		conn, err := api.NewDolphinDBClient(context.TODO(), host3, nil)
		So(err, ShouldBeNil)
		conn.Connect()
		loginReq := &api.LoginRequest{
			UserID:   "test1",
			Password: "123456",
		}
		err = conn.Login(loginReq)
		So(err, ShouldBeNil)
		res, _ := conn.RunScript("getConsoleJobs()")
		Println(res)
		So(res.(*model.Table).GetColumnByName("parallelism").Get(0).Value().(int32), ShouldEqual, 10)
		So(res.(*model.Table).GetColumnByName("priority").Get(0).Value().(int32), ShouldEqual, 4)

		conn.Close()
		So(conn.IsClosed(), ShouldBeTrue)
	})
	Convey("TestConnection_priority_10", t, func() {
		priority := 10
		opt := &dialer.BehaviorOptions{
			Priority: &priority,
		}
		conn, err := api.NewDolphinDBClient(context.TODO(), host3, opt)
		So(err, ShouldNotBeNil)
		result := fmt.Errorf("\n exception error is %w", err)
		fmt.Println(result.Error())
		expectedErrMsg := "the job priority must be between 0 and 8"
		So(result.Error(), ShouldContainSubstring, expectedErrMsg)
		if conn != nil {
			defer conn.Close() // 确保关闭连接
		}

	})

	db.Close()
	AssertEqual(db.IsClosed(), true)
}

func TestConnectionFetchSize(t *testing.T) {
	db, err := api.NewSimpleDolphinDBClient(context.TODO(), host3, "admin", "123456")
	AssertNil(err)
	Convey("Test_BehaviorOptions_FetchSize_Invalid", t, func() {

		FetchSize := 8191
		opt := &dialer.BehaviorOptions{
			FetchSize: &FetchSize,
		}
		conn, err := api.NewDolphinDBClient(context.TODO(), host3, opt)
		So(err, ShouldBeNil)
		err = conn.Connect()
		So(err, ShouldNotBeNil) // 确保连接返回了错误
		result := fmt.Errorf("\n exception error is %w", err)
		fmt.Println(result.Error())
		So(result, ShouldNotBeNil)
		conn.Close()
		So(conn.IsClosed(), ShouldBeTrue)
	})

	Convey("Test_BehaviorOptions_FetchSize_8192", t, func() {

		FetchSize := 8192
		opt := &dialer.BehaviorOptions{
			FetchSize: &FetchSize,
		}
		conn, err := api.NewDolphinDBClient(context.TODO(), host3, opt)
		So(err, ShouldBeNil)
		err = conn.Connect()
		So(err, ShouldBeNil)
	})

	db.Close()
	AssertEqual(db.IsClosed(), true)
}

func TestConnectionIsClearSessionMemory(t *testing.T) {
	db, err := api.NewSimpleDolphinDBClient(context.TODO(), host3, "admin", "123456")
	AssertNil(err)
	Convey("Test_BehaviorOptions_IsClearSessionMemory_true", t, func() {

		opt := &dialer.BehaviorOptions{
			IsClearSessionMemory: true,
		}
		conn, err := api.NewDolphinDBClient(context.TODO(), host3, opt)
		So(err, ShouldBeNil)
		conn.Connect()
		conn.RunScript("pt=table(1..3 as id);")
		_, err1 := conn.RunScript("select * from pt;")
		So(err1, ShouldNotBeNil)
		conn.Close()
		So(conn.IsClosed(), ShouldBeTrue)
	})

	Convey("Test_BehaviorOptions_IsClearSessionMemory_false", t, func() {

		opt := &dialer.BehaviorOptions{
			IsClearSessionMemory: false,
		}
		conn, err := api.NewDolphinDBClient(context.TODO(), host3, opt)
		So(err, ShouldBeNil)
		conn.Connect()
		conn.RunScript("pt=table(1..3 as id);")
		res, _ := conn.RunScript("select * from pt;")
		So(res.Rows(), ShouldEqual, 3)
		conn.Close()
		So(conn.IsClosed(), ShouldBeTrue)
	})

	db.Close()
	AssertEqual(db.IsClosed(), true)
}

func TestBehaviorOptions(t *testing.T) {
	Convey("Test_BehaviorOptions_Reconnect_false", t, func() {
		opt := &dialer.BehaviorOptions{
			Reconnect: false,
		}
		conn, _ := api.NewDolphinDBClient(context.TODO(), "192.168.0.69:3111", opt)
		err := conn.Connect()
		So(err, ShouldNotBeNil)
	})

	Convey("Test_BehaviorOptions_timeout_reached", t, func() {
		timeout := time.Second * 2
		opt := &dialer.BehaviorOptions{
			Timeout: timeout,
		}
		conn, _ := api.NewDolphinDBClient(context.TODO(), host3, opt)
		defer conn.Close()
		err := conn.Connect()
		So(err, ShouldBeNil)
		start := time.Now()
		_, err = conn.RunScript("sleep(3000)")
		So(err, ShouldNotBeNil)
		end := time.Now()
		So(end.Sub(start).Seconds(), ShouldBeGreaterThanOrEqualTo, 2)
		So(end.Sub(start).Seconds(), ShouldBeLessThan, 3)
	})
	Convey("Test_BehaviorOptions_timeout_not_reached", t, func() {
		timeout := time.Second * 2
		opt := &dialer.BehaviorOptions{
			Timeout: timeout,
		}
		conn, _ := api.NewDolphinDBClient(context.TODO(), host3, opt)
		defer conn.Close()
		err := conn.Connect()
		So(err, ShouldBeNil)
		start := time.Now()
		_, err = conn.RunScript("sleep(1000)")
		So(err, ShouldBeNil)
		end := time.Now()
		So(end.Sub(start).Seconds(), ShouldBeLessThan, 2)
		So(end.Sub(start).Seconds(), ShouldBeGreaterThanOrEqualTo, 1)
	})

	Convey("Test_BehaviorOptions_usePython_true", t, func() {
		opt := &dialer.BehaviorOptions{
			UsePython: true,
		}
		conn, _ := api.NewDolphinDBClient(context.TODO(), host3, opt)
		defer conn.Close()
		err := conn.Connect()
		So(err, ShouldBeNil)
		res, err := conn.RunScript("import dolphindb as ddb;import pandas as pd; type([1,2,3])")
		So(err, ShouldBeNil)
		So(res.(*model.Scalar).Value().(string), ShouldEqual, "list")
	})
}

func Test_Connection_SCRAM(t *testing.T) {
	db, err := api.NewSimpleDolphinDBClient(context.TODO(), host3, "admin", "123456")
	AssertNil(err)
	_, err = db.RunScript("try{deleteUser('scramUser')}catch(ex){};go;createUser(`scramUser, `123456, authMode='scram')")
	if err != nil {
		t.Skip("skip test because create SCRAM user failed")
	}
	Convey("Test_Connection_SCRAM_with_invalid_user", t, func() {
		opt := &dialer.BehaviorOptions{
			EnableScram: true,
		}
		conn, err := api.NewDolphinDBClient(context.TODO(), host3, opt)
		So(err, ShouldBeNil)
		conn.Connect()
		loginReq := &api.LoginRequest{
			UserID:   "admin",
			Password: "123456",
		}
		err = conn.Login(loginReq)
		So(err.Error(), ShouldEqual, "user 'admin' doesn't support scram authMode")
		res, _ := conn.RunScript("1+1")
		So(res.(*model.Scalar).Value().(int32), ShouldEqual, 2)
		conn.Close()
		So(conn.IsClosed(), ShouldBeTrue)
	})
	Convey("Test_NewDolphinDBClient_SCRAM_login_success", t, func() {
		opt := &dialer.BehaviorOptions{
			EnableScram: true,
		}
		conn, err := api.NewDolphinDBClient(context.TODO(), host3, opt)
		So(err, ShouldBeNil)
		conn.Connect()
		loginReq := &api.LoginRequest{
			UserID:   "scramUser",
			Password: "123456",
		}
		err = conn.Login(loginReq)
		So(err, ShouldBeNil)
		res, _ := conn.RunScript("1+1")
		So(res.(*model.Scalar).Value().(int32), ShouldEqual, 2)
		conn.Close()
		So(conn.IsClosed(), ShouldBeTrue)
	})
	Convey("Test_NewSimpleDolphinDBClient_SCRAM_user", t, func() {
		conn, err := api.NewSimpleDolphinDBClient(context.TODO(), host3, "scramUser", "123456")
		So(err, ShouldBeNil)
		conn.Connect()
		res, _ := conn.RunScript("1+1")
		So(res.(*model.Scalar).Value().(int32), ShouldEqual, 2)
		conn.Close()
	})

	db.Close()

}

func TestBehaviorOptions_NetTimeout(t *testing.T) {
	Convey("Test_BehaviorOptions_NetTimeout_negative", t, func() {
		opt := &dialer.BehaviorOptions{
			NetTimeout: -1 * time.Second,
		}
		_, err := api.NewDolphinDBClient(context.TODO(), setup.Address4, opt)
		So(err.Error(), ShouldContainSubstring, "the NetTimeout must be non-negative")
	})

	Convey("Test_BehaviorOptions_NetTimeout_0", t, func() {
		opt := &dialer.BehaviorOptions{
			NetTimeout: 0 * time.Second,
		}
		conn, _ := api.NewDolphinDBClient(context.TODO(), setup.Address4, opt)
		err := conn.Connect()
		So(err, ShouldBeNil)
	})
}

func TestNewDolphinDBClient_SqlStd(t *testing.T) {
	Convey("TestNewDolphinDBClient_SqlStd", t, func() {
		cases := []struct {
			name       string
			SqlStd     dialer.SqlStdEnum
			shouldFail bool
		}{
			{name: "default_dolphindb", SqlStd: dialer.SqlStdDolphinDB, shouldFail: true},
			{name: "oracle", SqlStd: dialer.SqlStdOracle, shouldFail: false},
			{name: "mysql", SqlStd: dialer.SqlStdMySQL, shouldFail: false},
		}

		for _, tc := range cases {
			tc := tc
			Convey(tc.name, func() {
				opt := (&dialer.BehaviorOptions{}).SetSqlStd(tc.SqlStd)
				conn, err := api.NewDolphinDBClient(context.TODO(), host3, opt)
				So(err, ShouldBeNil)

				err = conn.Connect()
				So(err, ShouldBeNil)
				defer conn.Close()

				loginReq := (&api.LoginRequest{}).
					SetUserID(setup.UserName).
					SetPassword(setup.Password)
				err = conn.Login(loginReq)
				So(err, ShouldBeNil)

				res, err := conn.RunScript("sysdate()")
				if tc.shouldFail {
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldContainSubstring, "sysdate")
					So(res, ShouldBeNil)
					return
				}

				So(err, ShouldBeNil)
				So(res, ShouldNotBeNil)
			})
		}
	})
}

func TestNewDolphinDBClient_tableInsert_haStreamTable(t *testing.T) {
	Convey("TestNewDolphinDBClient_tableInsert_haStreamTable", t, func() {
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
		// fmt.Println("---------------------------------Read data end------------------------------------")
		time.Sleep(3 * time.Second)
		// fmt.Println("Start Write!!!!!!!!!!!!!!!!!")
		for i := 0; i < 10; i++ {
			_, err = connection.RunFunc("tableInsert{st_scada_value}", values)
			So(err, ShouldBeNil)
			fmt.Println("数据插入", i, "次")
			//time.Sleep(3 * time.Second)
		}
		//fmt.Println("End Write!!!!!!!!!!!!!!!!!!!")
		res, err := connection.RunScript("select count(*) from st_scada_value")
		So(err, ShouldBeNil)
		So(res, ShouldNotBeNil)
		So(res.String(), ShouldContainSubstring, "100")
	})
}

func TestNewDolphinDBClient_tableInsert_haMvccTable_leader(t *testing.T) {
	Convey("TestNewDolphinDBClient_tableInsert_haMvccTable_leader", t, func() {
		conn, err := api.NewDolphinDBClient(context.TODO(), setup.Address, nil)
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)
		defer conn.Close()

		err = conn.Connect()
		So(err, ShouldBeNil)

		loginReq := (&api.LoginRequest{}).
			SetUserID(setup.UserName).
			SetPassword(setup.Password)
		err = conn.Login(loginReq)
		So(err, ShouldBeNil)

		leaderRes, err := conn.RunScript(" exec port from rpc(getControllerAlias(), getClusterPerf) where name=getHaMvccLeader(3);\n")
		So(err, ShouldBeNil)
		leaderPort := int(leaderRes.(*model.Vector).Get(0).Value().(int32))
		So(err, ShouldBeNil)

		leaderConn, err := api.NewDolphinDBClient(context.TODO(), setup.IP+":"+strconv.Itoa(leaderPort), nil)
		So(err, ShouldBeNil)
		So(leaderConn, ShouldNotBeNil)
		defer leaderConn.Close()

		err = leaderConn.Connect()
		So(err, ShouldBeNil)

		loginReq = (&api.LoginRequest{}).
			SetUserID(setup.UserName).
			SetPassword(setup.Password)
		err = leaderConn.Login(loginReq)
		So(err, ShouldBeNil)

		_, err = leaderConn.RunScript("try{dropHaMvccTable(\"HaMvccTable1\")}catch(ex){};\n go;\n haMvccTable(1:0, table(array(INT) as intv,array(SYMBOL) as symbolv),\"HaMvccTable1\",3)")
		So(err, ShouldBeNil)

		data, err := conn.RunScript("table(1..100 as intv,take(`qq`ee`rr,100) as symbolv)")
		So(err, ShouldBeNil)
		So(data, ShouldNotBeNil)

		values := []model.DataForm{data}
		_, err = leaderConn.RunFunc("tableInsert{loadHaMvccTable('HaMvccTable1')}", values)
		So(err, ShouldBeNil)

		res, err := leaderConn.RunScript("each(eqObj, (select * from loadHaMvccTable('HaMvccTable1')).values(), table(1..100 as intv,take(`qq`ee`rr,100) as symbolv).values()).all()")
		So(err, ShouldBeNil)
		So(res.(*model.Scalar).Value().(bool), ShouldEqual, true)
	})
}

func TestNewDolphinDBClient_tableInsert_haMvccTable_follower(t *testing.T) {
	Convey("TestNewDolphinDBClient_tableInsert_haMvccTable_follower", t, func() {
		conn, err := api.NewDolphinDBClient(context.TODO(), setup.Address, nil)
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)
		defer conn.Close()

		err = conn.Connect()
		So(err, ShouldBeNil)

		loginReq := (&api.LoginRequest{}).
			SetUserID(setup.UserName).
			SetPassword(setup.Password)
		err = conn.Login(loginReq)
		So(err, ShouldBeNil)

		leaderRes, err := conn.RunScript(" exec port from rpc(getControllerAlias(), getClusterPerf) where name=getHaMvccLeader(3);\n")
		So(err, ShouldBeNil)
		leaderPort, err := strconv.Atoi(leaderRes.(*model.Vector).Get(0).String())
		So(err, ShouldBeNil)

		followerRes, err := conn.RunScript(" exec port from rpc(getControllerAlias(), getClusterPerf) where name in (exec sites[0] from getHaMvccRaftGroups() where id==3).split(\",\") and name!=getHaMvccLeader(3) limit 1;\n")
		So(err, ShouldBeNil)
		followerPort := int(followerRes.(*model.Vector).Get(0).Value().(int32))
		So(err, ShouldBeNil)

		leaderConn, err := api.NewDolphinDBClient(context.TODO(), setup.IP+":"+strconv.Itoa(leaderPort), nil)
		So(err, ShouldBeNil)
		So(leaderConn, ShouldNotBeNil)
		defer leaderConn.Close()

		err = leaderConn.Connect()
		So(err, ShouldBeNil)

		loginReq = (&api.LoginRequest{}).
			SetUserID(setup.UserName).
			SetPassword(setup.Password)
		err = leaderConn.Login(loginReq)
		So(err, ShouldBeNil)

		opt := &dialer.BehaviorOptions{
			EnableHighAvailability: true,
			HighAvailabilitySites:  []string{setup.Address, setup.Address2, setup.Address3},
		}
		followerConn, err := api.NewDolphinDBClient(context.TODO(), setup.IP+":"+strconv.Itoa(followerPort), opt)
		So(err, ShouldBeNil)
		So(followerConn, ShouldNotBeNil)
		defer followerConn.Close()

		err = followerConn.Connect()
		So(err, ShouldBeNil)

		loginReq = (&api.LoginRequest{}).
			SetUserID(setup.UserName).
			SetPassword(setup.Password)
		err = followerConn.Login(loginReq)
		So(err, ShouldBeNil)

		_, err = leaderConn.RunScript("try{dropHaMvccTable(\"HaMvccTable1\")}catch(ex){};\n go;\n haMvccTable(1:0, table(array(INT) as intv,array(SYMBOL) as symbolv),\"HaMvccTable1\",3)")
		time.Sleep(1 * time.Second)
		So(err, ShouldBeNil)

		data, err := conn.RunScript("table(1..100 as intv,take(`qq`ee`rr,100) as symbolv)")
		So(err, ShouldBeNil)
		So(data, ShouldNotBeNil)

		values := []model.DataForm{data}
		_, err = followerConn.RunFunc("tableInsert{loadHaMvccTable('HaMvccTable1')}", values)
		So(err, ShouldBeNil)
		time.Sleep(1 * time.Second)
		res, err := followerConn.RunScript("each(eqObj, (select * from loadHaMvccTable('HaMvccTable1')).values(), table(1..100 as intv,take(`qq`ee`rr,100) as symbolv).values()).all()")
		So(err, ShouldBeNil)
		So(res.(*model.Scalar).Value().(bool), ShouldEqual, true)
	})
}

func TestNewDolphinDBClient_tableInsert_haStreamTable_leader(t *testing.T) {
	Convey("TestNewDolphinDBClient_tableInsert_haStreamTable_leader", t, func() {
		conn, err := api.NewDolphinDBClient(context.TODO(), setup.Address, nil)
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)
		defer conn.Close()

		err = conn.Connect()
		So(err, ShouldBeNil)

		loginReq := (&api.LoginRequest{}).
			SetUserID(setup.UserName).
			SetPassword(setup.Password)
		err = conn.Login(loginReq)
		So(err, ShouldBeNil)

		leaderRes, err := conn.RunScript(" exec port from rpc(getControllerAlias(), getClusterPerf) where name=getStreamingLeader(11);\n")
		So(err, ShouldBeNil)
		leaderPort := int(leaderRes.(*model.Vector).Get(0).Value().(int32))
		So(err, ShouldBeNil)

		leaderConn, err := api.NewDolphinDBClient(context.TODO(), setup.IP+":"+strconv.Itoa(leaderPort), nil)
		So(err, ShouldBeNil)
		So(leaderConn, ShouldNotBeNil)
		defer leaderConn.Close()

		err = leaderConn.Connect()
		So(err, ShouldBeNil)

		loginReq = (&api.LoginRequest{}).
			SetUserID(setup.UserName).
			SetPassword(setup.Password)
		err = leaderConn.Login(loginReq)
		So(err, ShouldBeNil)

		_, err = leaderConn.RunScript("try{dropStreamTable(\"haStreamTable1\")}catch(ex){};\n go;\n haStreamTable(11, table(array(INT) as intv,array(SYMBOL) as symbolv),\"haStreamTable1\",100000)")
		So(err, ShouldBeNil)

		data, err := conn.RunScript("table(1..100 as intv,take(`qq`ee`rr,100) as symbolv)")
		So(err, ShouldBeNil)
		So(data, ShouldNotBeNil)

		values := []model.DataForm{data}
		_, err = leaderConn.RunFunc("tableInsert{haStreamTable1}", values)
		So(err, ShouldBeNil)

		res, err := leaderConn.RunScript("each(eqObj, (select * from haStreamTable1).values(), table(1..100 as intv,take(`qq`ee`rr,100) as symbolv).values()).all()")
		So(err, ShouldBeNil)
		So(res.(*model.Scalar).Value().(bool), ShouldBeTrue)
	})
}

func TestNewDolphinDBClient_tableInsert_haStreamTable_follower(t *testing.T) {
	Convey("TestNewDolphinDBClient_tableInsert_haStreamTable_follower", t, func() {
		conn, err := api.NewDolphinDBClient(context.TODO(), setup.Address, nil)
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)
		defer conn.Close()

		err = conn.Connect()
		So(err, ShouldBeNil)

		loginReq := (&api.LoginRequest{}).
			SetUserID(setup.UserName).
			SetPassword(setup.Password)
		err = conn.Login(loginReq)
		So(err, ShouldBeNil)

		leaderRes, err := conn.RunScript(" exec port from rpc(getControllerAlias(), getClusterPerf) where name=getStreamingLeader(11);\n")
		So(err, ShouldBeNil)
		leaderPort := int(leaderRes.(*model.Vector).Get(0).Value().(int32))
		So(err, ShouldBeNil)

		followerRes, err := conn.RunScript("tmp1=(exec sites[0] from getStreamingRaftGroups() where raftGroupName==\"11\").split(\",\");\ntmp2=each(x->split(x, \":\")[2],tmp1);\nexec port from rpc(getControllerAlias(), getClusterPerf) where name in tmp2  and name!=getStreamingLeader(11) limit 1;\n")
		So(err, ShouldBeNil)
		followerPort := int(followerRes.(*model.Vector).Get(0).Value().(int32))
		So(err, ShouldBeNil)

		leaderConn, err := api.NewDolphinDBClient(context.TODO(), setup.IP+":"+strconv.Itoa(leaderPort), nil)
		So(err, ShouldBeNil)
		So(leaderConn, ShouldNotBeNil)
		defer leaderConn.Close()

		err = leaderConn.Connect()
		So(err, ShouldBeNil)

		loginReq = (&api.LoginRequest{}).
			SetUserID(setup.UserName).
			SetPassword(setup.Password)
		err = leaderConn.Login(loginReq)
		So(err, ShouldBeNil)

		opt := &dialer.BehaviorOptions{
			EnableHighAvailability: true,
			HighAvailabilitySites:  []string{setup.Address, setup.Address2, setup.Address3},
		}
		followerConn, err := api.NewDolphinDBClient(context.TODO(), setup.IP+":"+strconv.Itoa(followerPort), opt)
		So(err, ShouldBeNil)
		So(followerConn, ShouldNotBeNil)
		defer followerConn.Close()

		err = followerConn.Connect()
		So(err, ShouldBeNil)

		loginReq = (&api.LoginRequest{}).
			SetUserID(setup.UserName).
			SetPassword(setup.Password)
		err = followerConn.Login(loginReq)
		So(err, ShouldBeNil)

		_, err = leaderConn.RunScript("try{dropStreamTable(\"haStreamTable1\")}catch(ex){};\n go;\n haStreamTable(11, table(array(INT) as intv,array(SYMBOL) as symbolv),\"haStreamTable1\",100000)")
		time.Sleep(1 * time.Second)
		So(err, ShouldBeNil)

		data, err := conn.RunScript("table(1..100 as intv,take(`qq`ee`rr,100) as symbolv)")
		So(err, ShouldBeNil)
		So(data, ShouldNotBeNil)

		values := []model.DataForm{data}
		_, err = followerConn.RunFunc("tableInsert{haStreamTable1}", values)
		So(err, ShouldBeNil)

		res, err := followerConn.RunScript("each(eqObj, (select * from haStreamTable1).values(), table(1..100 as intv,take(`qq`ee`rr,100) as symbolv).values()).all()")
		So(err, ShouldBeNil)
		So(res.(*model.Scalar).Value().(bool), ShouldBeTrue)
	})
}
