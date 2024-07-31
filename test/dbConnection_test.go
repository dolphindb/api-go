package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/dialer"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
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
		})

		Convey("Test NewSimpleDolphinDB logout", func() {
			db, _ := api.NewSimpleDolphinDBClient(context.TODO(), host3, setup.UserName, setup.Password)
			err := db.Logout()
			So(err, ShouldBeNil)
			re, err := db.RunScript(`
			dbName="dfs://` + generateRandomString(10) + `"
			if(existsDatabase(dbName)){
				dropDatabase(dbName)
			}
			db=database(dbName, VALUE, 1..10)`)
			result := fmt.Errorf("\n error is %w", err)
			So(re, ShouldBeNil)
			So(result, ShouldNotBeNil)
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
	t.Parallel()
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

}

func TestConnectionParallel(t *testing.T) {
	t.Parallel()
	db, err := api.NewSimpleDolphinDBClient(context.TODO(), host3, "admin", "123456")
	AssertNil(err)
	db.RunScript("login(`admin,`123456);try{createUser(`test1, `123456)}catch(ex){};go;setMaxJobParallelism(`test1, 10);")
	Convey("TestConnectionParallel_lt_MaxJobParallelism", t, func() {

		priority := 4
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
		So(res.(*model.Table).GetColumnByName("priority").Get(0).Value().(int32), ShouldEqual, 4)

		conn.Close()
		So(conn.IsClosed(), ShouldBeTrue)
	})

	Convey("TestConnectionParallel_gt_MaxJobParallelism", t, func() {

		priority := 4
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
		So(res.(*model.Table).GetColumnByName("priority").Get(0).Value().(int32), ShouldEqual, 4)

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

	db.Close()
	AssertEqual(db.IsClosed(), true)
}
