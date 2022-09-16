package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/dialer"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

func TestNewDolphinDBClient(t *testing.T) {
	Convey("func NewDolphinDB exception test", t, func() {
		Convey("Test NewDolphinDB wrong address exception", func() {
			_, err := api.NewDolphinDBClient(context.TODO(), "123456", nil)
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})

		Convey("Test NewDolphinDB login wrong userName exception", func() {
			db, _ := api.NewDolphinDBClient(context.TODO(), setup.Address, nil)
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
			db, _ := api.NewDolphinDBClient(context.TODO(), setup.Address, nil)
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
			db, err := api.NewDolphinDBClient(context.TODO(), setup.Address, nil)
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
			db, _ := api.NewDolphinDBClient(context.TODO(), setup.Address, nil)
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
	Convey("func NewSimpleDolphinDB exception test", t, func() {
		Convey("Test NewSimpleDolphinDB wrong address exception", func() {
			_, err := api.NewSimpleDolphinDBClient(context.TODO(), "wrongAddress", setup.UserName, setup.Password)
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})

		Convey("Test NewSimpleDolphinDB wrong userName int exception", func() {
			_, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, "1234", setup.Password)
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})

		Convey("Test NewSimpleDolphinDB wrong password exception", func() {
			_, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, "12")
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})
	})

	Convey("Test NewSimpleDolphinDB login and logout", t, func() {
		Convey("Test NewSimpleDolphinDB login", func() {
			db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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
		})

		Convey("Test NewSimpleDolphinDB logout", func() {
			db, _ := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
			err := db.Logout()
			So(err, ShouldBeNil)
			re, err := db.RunScript(`
			dbName="dfs://test"
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
	Convey("Test connection Close", t, func() {
		Convey("Test NewDolphinDB Close", func() {
			db, err := api.NewDolphinDBClient(context.TODO(), setup.Address, nil)
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
			db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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
	Convey("Test connection IsClosed", t, func() {
		Convey("Test NewDolphinDB IsClosed", func() {
			db, err := api.NewDolphinDBClient(context.TODO(), setup.Address, nil)
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
			db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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
	Convey("Test RefreshTimeout NewSimpleConn", t, func() {
		db, err := dialer.NewSimpleConn(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		SessionID1 := db.GetSession()
		So(SessionID1, ShouldNotBeNil)
		db.RefreshTimeout(100)
		SessionID2 := db.GetSession()
		So(SessionID1, ShouldEqual, SessionID2)
		db.Close()
	})
	Convey("Test RefreshTimeout NewConn", t, func() {
		db, err := dialer.NewConn(context.TODO(), setup.Address, nil)
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
	Convey("Test connection GetSession", t, func() {
		Convey("Test NewDolphinDB GetSession", func() {
			db, err := api.NewDolphinDBClient(context.TODO(), setup.Address, nil)
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
			db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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
	Convey("func NewConn exception test", t, func() {
		Convey("Test NewConn wrong address exception", func() {
			_, err := dialer.NewConn(context.TODO(), "123456", nil)
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})
	})
	Convey("Test NewConn connection", t, func() {
		db, err := dialer.NewConn(context.TODO(), setup.Address, nil)
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
	Convey("func NewSimpleConn exception test", t, func() {
		Convey("Test NewSimpleConn wrong address exception", func() {
			_, err := dialer.NewSimpleConn(context.TODO(), "wrongAddress", setup.UserName, setup.Password)
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})

		Convey("Test NewSimpleConn wrong userName int exception", func() {
			_, err := dialer.NewSimpleConn(context.TODO(), setup.Address, "1234", setup.Password)
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})

		Convey("Test NewSimpleConn wrong password exception", func() {
			_, err := dialer.NewSimpleConn(context.TODO(), setup.Address, setup.UserName, "12")
			result := fmt.Errorf("\n exception error is %w", err)
			fmt.Println(result.Error())
			So(result, ShouldNotBeNil)
		})
	})

	Convey("Test NewSimpleConn login and logout", t, func() {
		Convey("Test NewSimpleConn login", func() {
			db, err := dialer.NewSimpleConn(context.TODO(), setup.Address, setup.UserName, setup.Password)
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
			db, _ := dialer.NewSimpleConn(context.TODO(), setup.Address, setup.UserName, setup.Password)
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
			err = db.Close()
			So(err, ShouldBeNil)
			add := db.GetLocalAddress()
			So(add, ShouldEqual, setup.IP)
			db.Close()
		})
	})
}

func TestGetLocalAddress(t *testing.T) {
	Convey("Test GetLocalAddress NewSimpleConn", t, func() {
		db, err := dialer.NewSimpleConn(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		re := db.GetLocalAddress()
		So(re, ShouldEqual, setup.IP)
		db.Close()
	})
	Convey("Test GetLocalAddress NewConn", t, func() {
		db, err := dialer.NewConn(context.TODO(), setup.Address, nil)
		So(err, ShouldBeNil)
		re := db.GetLocalAddress()
		So(re, ShouldEqual, setup.IP)
		db.Close()
	})
	Convey("Test GetLocalAddress NewSimpleDolphinDBClient", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		re := db.GetLocalAddress()
		So(re, ShouldEqual, setup.IP)
		db.Close()
	})
	Convey("Test GetLocalAddress NewDolphinDBClient", t, func() {
		db, err := api.NewDolphinDBClient(context.TODO(), setup.Address, nil)
		So(err, ShouldBeNil)
		re := db.GetLocalAddress()
		So(re, ShouldEqual, setup.IP)
		db.Close()
	})
}
