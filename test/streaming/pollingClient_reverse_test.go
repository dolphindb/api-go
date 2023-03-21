package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/streaming"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

var pc_r = streaming.NewPollingClient(setup.IP, setup.Reverse_subPort)

func CreateStreamingTable_r() {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
	AssertNil(err)
	script := "try{ dropStreamTable(`tradesTable) }catch(ex){};go;" +
		"share streamTable(10000:0,`timev`sym`pricev, [TIMESTAMP,SYMBOL,DOUBLE]) as tradesTable;"
	_, err = ddb.RunScript(script)
	AssertNil(err)
	err = ddb.Close()
	AssertNil(err)
}

func TestSubscribe_exception_r(t *testing.T) {
	Convey("Test_subscribe_exception", t, func() {
		Convey("Test_AbstractClient_shared_table_polling_doesnot_exist_exception", func() {
			req := &streaming.SubscribeRequest{
				Address:    setup.Address,
				TableName:  "polling",
				ActionName: "action1",
				Offset:     0,
				Reconnect:  true,
			}
			_, err := pc_r.Subscribe(req)
			So(err, ShouldNotBeNil)
		})
		Convey("Test_subscribe_err_host", func() {
			req := &streaming.SubscribeRequest{
				Address:    "200.48.100.451:8876",
				TableName:  "polling",
				ActionName: "action1",
				Offset:     0,
				Reconnect:  true,
			}
			_, err := pc_r.Subscribe(req)
			So(err, ShouldNotBeNil)
		})
		Convey("Test_subscribe_err_port", func() {
			req := &streaming.SubscribeRequest{
				Address:    setup.IP + ":8876",
				TableName:  "polling",
				ActionName: "action1",
				Offset:     0,
				Reconnect:  true,
			}
			_, err := pc_r.Subscribe(req)
			So(err, ShouldNotBeNil)
		})
		Convey("Test_subscribe_err_TableName", func() {
			req := &streaming.SubscribeRequest{
				Address:    setup.Address,
				TableName:  "",
				ActionName: "action1",
				Offset:     0,
				Reconnect:  true,
			}
			_, err := pc_r.Subscribe(req)
			So(err, ShouldNotBeNil)
		})
		Convey("Test_subscribe_ActionName_null", func() {
			req := &streaming.SubscribeRequest{
				Address:    setup.Address,
				ActionName: "",
				TableName:  "polling",
				Offset:     0,
				Reconnect:  true,
			}
			_, err := pc_r.Subscribe(req)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestPollingClient_r(t *testing.T) {
	Convey("Test_PollingClient_test_size", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		CreateStreamingTable_r()
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "tradesTable",
			ActionName: "action1",
			Offset:     0,
			Reconnect:  true,
		}
		poller, err := pc_r.Subscribe(req)
		So(err, ShouldBeNil)
		Convey("Test_GetTopicPoller_exitsing_data", func() {
			msg := poller.Poll(1000, 10)
			So(len(msg), ShouldEqual, 0)
		})
		Convey("Test_poll_size_sub_data", func() {
			for i := 0; i < 10; i++ { //data<size
				_, err = ddb.RunScript("insert into tradesTable values(take(now(), 50), take(`000905`600001`300201`000908`600002, 50), rand(1000,50)/10.0);")
				AssertNil(err)
				msg := poller.Poll(100, 1000)
				if msg == nil {
					continue
				} else if len(msg) > 0 {
					So(len(msg), ShouldEqual, 50)
				}
			}
			for i := 0; i < 10; i++ { //data>size
				_, err = ddb.RunScript("dataNum=5000;insert into tradesTable values(take(now(), dataNum), take(`000905`600001`300201`000908`600002, dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				msg := poller.Poll(100000, 1000)
				if msg == nil {
					continue
				} else if len(msg) > 0 {
					So(len(msg), ShouldBeGreaterThanOrEqualTo, 1000)
				}
			}
			for i := 0; i < 10; i++ { //data=size
				_, err = ddb.RunScript("dataNum=5000;insert into tradesTable values(take(now(), dataNum), take(`000905`600001`300201`000908`600002, dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				msg := poller.Poll(100000, 5000)
				if msg == nil {
					continue
				} else if len(msg) > 0 {
					So(len(msg), ShouldEqual, 5000)
				}
			}
			for i := 0; i < 10; i++ { //bigsize
				_, err = ddb.RunScript("dataNum=5000;insert into tradesTable values(take(now(), dataNum), take(`000905`600001`300201`000908`600002, dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				msg := poller.Poll(1000, 1000000000)
				if msg == nil {
					continue
				} else if len(msg) > 0 {
					So(len(msg), ShouldBeGreaterThanOrEqualTo, 5000)
				}
			}
			for i := 0; i < 10; i++ { //bigData
				_, err = ddb.RunScript("dataNum=10000000;insert into tradesTable values(take(now(), dataNum), take(`000905`600001`300201`000908`600002, dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				msg := poller.Poll(1000, 10000)
				if msg == nil {
					continue
				} else if len(msg) > 0 {
					So(len(msg), ShouldBeGreaterThanOrEqualTo, 10000)
				}
			}
			for i := 0; i < 10; i++ { //smallData
				_, err = ddb.RunScript("dataNum=1;insert into tradesTable values(take(now(), dataNum), take(`000905`600001`300201`000908`600002, dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				msg := poller.Poll(1000, 10000)
				if msg == nil {
					continue
				} else if len(msg) > 0 {
					So(len(msg), ShouldBeGreaterThanOrEqualTo, 1)
				}
			}
			for i := 0; i < 10; i++ { //append Many times
				_, err = ddb.RunScript("dataNum=10;insert into tradesTable values(take(now(), dataNum), take(`000905`600001`300201`000908`600002, dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				_, err = ddb.RunScript("dataNum=20;insert into tradesTable values(take(now(), dataNum), take(`000905`600001`300201`000908`600002, dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				_, err = ddb.RunScript("dataNum=30;insert into tradesTable values(take(now(), dataNum), take(`000905`600001`300201`000908`600002, dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				_, err = ddb.RunScript("dataNum=40;insert into tradesTable values(take(now(), dataNum), take(`000905`600001`300201`000908`600002, dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				_, err = ddb.RunScript("dataNum=50;insert into tradesTable values(take(now(), dataNum), take(`000905`600001`300201`000908`600002, dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				msg := poller.Poll(1000, 10000)
				if msg == nil {
					continue
				} else if len(msg) > 0 {
					So(len(msg), ShouldBeGreaterThanOrEqualTo, 100)
				}
			}
			for i := 0; i < 10; i++ { //size=0
				_, err = ddb.RunScript("dataNum=100;insert into tradesTable values(take(now(), dataNum), take(`000905`600001`300201`000908`600002, dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				msg := poller.Poll(1000, 0)
				if msg == nil {
					continue
				} else if len(msg) > 0 {
					So(len(msg), ShouldEqual, 100)
				}
			}
			for i := 0; i < 10; i++ { //size<0
				_, err = ddb.RunScript("dataNum=100;insert into tradesTable values(take(now(), dataNum), take(`000905`600001`300201`000908`600002, dataNum), rand(1000,dataNum)/10.0);")
				So(err, ShouldBeNil)
				msg := poller.Poll(1000, -10)
				if msg == nil {
					continue
				} else if len(msg) > 0 {
					So(len(msg), ShouldEqual, 100)
				}
			}
		})
		err = pc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
	})
}

func TestSubsribe_size_r(t *testing.T) {
	Convey("TestSubsribe_size", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		CreateStreamingTable_r()
		req1 := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "tradesTable",
			ActionName: "subtrades1",
			Offset:     0,
			Reconnect:  true,
		}
		poller1, err := pc_r.Subscribe(req1)
		So(err, ShouldBeNil)
		So(err, ShouldBeNil)
		for i := 0; i < 10; i++ {
			_, err = ddb.RunScript("dataNum=1000;insert into tradesTable values(take(now(), dataNum), take(`000905`600001`300201`000908`600002, dataNum), rand(1000,dataNum)/10.0);")
			So(err, ShouldBeNil)
			rows, _ := ddb.RunScript("exec count(*) from tradesTable")
			fmt.Print(rows)
			msg1 := poller1.Poll(1000, 1000)
			if msg1 == nil {
				continue
			} else if len(msg1) > 0 {
				So(len(msg1), ShouldBeGreaterThanOrEqualTo, 1000)
			}

		}
		err = pc_r.UnSubscribe(req1)
		So(err, ShouldBeNil)
		So(ddb.Close(), ShouldBeNil)
	})
}

func TestSubsribe_take_r(t *testing.T) {
	Convey("TestSubsribe_take", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		script := "try{ dropStreamTable(`tradesTable1)}catch(ex){};" +
			"share streamTable(10000:0,`timev`sym`pricev, [TIMESTAMP,SYMBOL,DOUBLE]) as tradesTable1;"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		req := &streaming.SubscribeRequest{
			Address:    setup.Address,
			TableName:  "tradesTable1",
			ActionName: "subtrades1",
			Offset:     0,
			Reconnect:  false,
		}
		poller3, err := pc_r.Subscribe(req)
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dataNum=1; insert into tradesTable1 values(take(now(), dataNum), take(`000905`600001`300201`000908`600002, dataNum), rand(1000,dataNum)/10.0);")
		So(err, ShouldBeNil)
		IMessage := poller3.Take()
		Topicmsg := IMessage.GetTopic()
		fmt.Println(Topicmsg)
		SubscriptionTopic, err := ddb.RunScript("getSubscriptionTopic(tableName=\"tradesTable1\", actionName=\"subtrades1\")")
		exTopic := SubscriptionTopic.(*model.Vector).Get(0).String()
		So(err, ShouldBeNil)
		So(exTopic, ShouldEqual, "string("+Topicmsg+")")
		Offset := IMessage.GetOffset()
		So(Offset, ShouldEqual, 0)
		tmp, err := ddb.RunScript("select * from tradesTable1")
		exTable := tmp.(*model.Table)
		So(err, ShouldBeNil)
		retimev := IMessage.GetValue(0).(*model.Vector).String()
		resymbol := IMessage.GetValue(1).(*model.Vector).String()
		repricev := IMessage.GetValue(2).(*model.Vector).String()
		extimev := exTable.GetColumnByIndex(0).Get(0).String()
		exsymbol := exTable.GetColumnByIndex(1).Get(0).String()
		expricev := exTable.GetColumnByIndex(2).Get(0).String()
		retimev1 := IMessage.GetValueByName("timev").String()
		resymbol1 := IMessage.GetValueByName("sym").String()
		repricev1 := IMessage.GetValueByName("pricev").String()
		So(retimev, ShouldEqual, "vector<timestamp>(["+extimev+"])")
		So(resymbol, ShouldEqual, "vector<string>(["+exsymbol+"])")
		So(repricev, ShouldEqual, "vector<double>(["+expricev+"])")
		So(retimev1, ShouldEqual, "vector<timestamp>(["+extimev+"])")
		So(resymbol1, ShouldEqual, "vector<string>(["+exsymbol+"])")
		So(repricev1, ShouldEqual, "vector<double>(["+expricev+"])")
		err = pc_r.UnSubscribe(req)
		So(err, ShouldBeNil)
		So(ddb.Close(), ShouldBeNil)
	})
}

func TestPollingClientClose_r(t *testing.T) {
	Convey("TestPollingClientClose", t, func() {
		IsClosed := pc_r.IsClosed()
		So(IsClosed, ShouldBeFalse)
		pc_r.Close()
		IsClosed = pc_r.IsClosed()
		So(IsClosed, ShouldBeTrue)
	})
}
