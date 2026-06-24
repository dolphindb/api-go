package test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/dolphindb/api-go/v3/dolphindb"
	"github.com/dolphindb/api-go/v3/logging"
	"github.com/dolphindb/api-go/v3/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

func TestNewClient(t *testing.T) {
	t.Parallel()
	logging.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))

	Convey("NewClient_exception_test", t, func() {
		Convey("Test_NewClient_address_wrong_exception", func() {
			client, err := dolphindb.NewClient(context.TODO(), "address_wrong", nil)
			So(err, ShouldBeNil)
			So(client, ShouldNotBeNil)
			defer client.Close()

			conErr := client.Connect()
			So(conErr, ShouldNotBeNil)
			So(conErr.Error(), ShouldContainSubstring, "failed to connect to address_wrong")
		})

		Convey("Test_NewClient_login_userName_wrong_exception", func() {
			client, err := dolphindb.NewClient(context.TODO(), setup.Address, nil)
			So(err, ShouldBeNil)
			So(client, ShouldNotBeNil)
			defer client.Close()

			err = client.Connect()
			So(err, ShouldBeNil)

			loginReq := new(dolphindb.LoginRequest).
				SetUserID("wrongName").
				SetPassword(setup.Password)
			err = client.Login(loginReq)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "The user name or password is incorrect")
		})

		Convey("Test_NewClient_login_password_wrong_exception", func() {
			client, err := dolphindb.NewClient(context.TODO(), setup.Address, nil)
			So(err, ShouldBeNil)
			So(client, ShouldNotBeNil)
			defer client.Close()

			err = client.Connect()
			So(err, ShouldBeNil)

			loginReq := new(dolphindb.LoginRequest).
				SetUserID(setup.UserName).
				SetPassword("wrong password")
			err = client.Login(loginReq)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "The user name or password is incorrect")
		})
	})

	Convey("NewClient_login_and_logout", t, func() {
		Convey("Test_NewClient_login_success", func() {
			client, err := dolphindb.NewClient(context.TODO(), setup.Address, nil)
			So(err, ShouldBeNil)
			So(client, ShouldNotBeNil)
			defer client.Close()

			err = client.Connect()
			So(err, ShouldBeNil)

			loginReq := new(dolphindb.LoginRequest).
				SetUserID(setup.UserName).
				SetPassword(setup.Password)
			err = client.Login(loginReq)
			So(err, ShouldBeNil)
		})

		Convey("Test_NewClient_logout_success", func() {
			client, err := dolphindb.NewClient(context.TODO(), setup.Address, nil)
			So(err, ShouldBeNil)
			So(client, ShouldNotBeNil)
			defer client.Close()

			err = client.Connect()
			So(err, ShouldBeNil)

			loginReq := new(dolphindb.LoginRequest).
				SetUserID(setup.UserName).
				SetPassword(setup.Password)
			err = client.Login(loginReq)
			So(err, ShouldBeNil)

			err = client.Logout()
			So(err, ShouldBeNil)
		})
	})
}

func TestDial(t *testing.T) {
	t.Parallel()

	Convey("Dial_exception_test", t, func() {
		Convey("Test_Dial_address_wrong_exception", func() {
			client, err := dolphindb.Dial("address_wrong", "user", "password", nil)
			So(err, ShouldNotBeNil)
			So(client, ShouldBeNil)
			So(err.Error(), ShouldContainSubstring, "failed to connect to address_wrong")
		})

		Convey("Test_Dial_login_userName_wrong_exception", func() {
			client, err := dolphindb.Dial(setup.Address, "wrongName", setup.Password, nil)
			So(err, ShouldNotBeNil)
			So(client, ShouldBeNil)
			So(err.Error(), ShouldContainSubstring, "The user name or password is incorrect")
		})

		Convey("Test_Dial_login_password_wrong_exception", func() {
			client, err := dolphindb.Dial(setup.Address, setup.UserName, "wrong password", nil)
			So(err, ShouldNotBeNil)
			So(client, ShouldBeNil)
			So(err.Error(), ShouldContainSubstring, "The user name or password is incorrect")
		})
	})

	Convey("Dial_login_and_logout", t, func() {
		Convey("Test_Dial_login_success", func() {
			client, err := dolphindb.Dial(setup.Address, setup.UserName, setup.Password, nil)
			So(err, ShouldBeNil)
			So(client, ShouldNotBeNil)
			defer client.Close()
			So(client.IsConnected(), ShouldBeTrue)
			_, er := client.RunScript("1+1;")
			So(er, ShouldBeNil)
		})

		Convey("Test_Dial_logout_success", func() {
			client, err := dolphindb.Dial(setup.Address, setup.UserName, setup.Password, nil)
			So(err, ShouldBeNil)
			So(client, ShouldNotBeNil)
			defer client.Close()
			So(client.IsConnected(), ShouldBeTrue)
			So(client.GetUserID(), ShouldEqual, setup.UserName)
			So(client.GetPassword(), ShouldEqual, setup.Password)
			_, er := client.RunScript("getGroupList();")
			So(er, ShouldBeNil)
			err = client.Logout()
			So(err, ShouldBeNil)
			_, er1 := client.RunScript("getGroupList();")
			So(er1, ShouldNotBeNil)
			So(er1.Error(), ShouldContainSubstring, "Only administrators execute function getGroupList")
		})
	})
}

func TestClientMethods(t *testing.T) {
	t.Parallel()
	logging.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))

	Convey("Client_Methods_test", t, func() {
		client, err := dolphindb.Dial(setup.Address, setup.UserName, setup.Password, nil)
		So(err, ShouldBeNil)
		So(client, ShouldNotBeNil)
		So(client.GetLocalAddress(), ShouldNotBeEmpty)
		So(client.GetTCPConn(), ShouldNotBeNil)
		So(client.GetSession(), ShouldNotBeEmpty)
		So(client.GetUserID(), ShouldEqual, "admin")
		So(client.GetPassword(), ShouldEqual, "123456")

		script := "def create_user(){try{deleteUser(`user1)}catch(ex){};createUser(`user1,'123456',,true);};" +
			"rpc(getControllerAlias(),create_user);"
		client.RunScript(script)
		client.SetUserID("user1")
		client.SetPassword("123456")
		So(client.GetUserID(), ShouldEqual, "user1")
		So(client.GetPassword(), ShouldEqual, "123456")

		client.Close()
		So(client.IsClosed(), ShouldBeTrue)
	})
}
