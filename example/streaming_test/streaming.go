package streaming_test

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/dolphindb/api-go/v3/api"
	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/dolphindb/api-go/v3/example/apis"
	"github.com/dolphindb/api-go/v3/example/util"
	"github.com/dolphindb/api-go/v3/logging"
	"github.com/dolphindb/api-go/v3/model"
	"github.com/dolphindb/api-go/v3/streaming"
)

var streamConn dialer.Conn

type sampleHandler1 struct{}

func (s *sampleHandler1) DoEvent(msg streaming.IMessage) {
	val0 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val1 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val2 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val3 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val4 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val5 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val6 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val7 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val8 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val9 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val10 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val11 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val12 := msg.GetValue(0).(*model.Scalar).DataType.String()

	script := fmt.Sprintf("insert into sub1 values(%s,%s,\"%s\",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
		val0, val1, val2, val3, val4, val5, val6, val7, val8, val9, val10, val11, val12)

	_, err := streamConn.RunScript(script)
	util.AssertNil(err)
}

type sampleHandler2 struct{}

func (s *sampleHandler2) DoEvent(msg streaming.IMessage) {
	val0 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val1 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val2 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val3 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val4 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val5 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val6 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val7 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val8 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val9 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val10 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val11 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val12 := msg.GetValue(0).(*model.Scalar).DataType.String()

	script := fmt.Sprintf("insert into sub2 values(%s,%s,\"%s\",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
		val0, val1, val2, val3, val4, val5, val6, val7, val8, val9, val10, val11, val12)

	_, err := streamConn.RunScript(script)
	util.AssertNil(err)
}

func prepareStreamTable(db api.DolphinDB, tableName string) {
	script := fmt.Sprintf("share(streamTable(1000000:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]), \"%s\")", tableName)
	_, err := db.RunScript(script)
	util.AssertNil(err)
}

func writeStreamTable(db api.DolphinDB, tableName string, batch int) {
	buf := bytes.NewBufferString(fmt.Sprintf("tmp = table(%d:%d,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);", batch, batch))
	buf.WriteString(fmt.Sprintf("tmp[`permno] = rand(1000, %d);", batch))
	buf.WriteString(fmt.Sprintf("tmp[`timestamp] = take(now(), %d);", batch))
	buf.WriteString(fmt.Sprintf("tmp[`ticker] = rand(\"A\"+string(1..1000), %d);", batch))
	buf.WriteString(fmt.Sprintf("tmp[`price1] = rand(100, %d);", batch))
	buf.WriteString(fmt.Sprintf("tmp[`price2] = rand(100, %d);", batch))
	buf.WriteString(fmt.Sprintf("tmp[`price3] = rand(100, %d);", batch))
	buf.WriteString(fmt.Sprintf("tmp[`price4] = rand(100, %d);", batch))
	buf.WriteString(fmt.Sprintf("tmp[`price5] = rand(100, %d);", batch))
	buf.WriteString(fmt.Sprintf("tmp[`vol1] = rand(100, %d);", batch))
	buf.WriteString(fmt.Sprintf("tmp[`vol2] = rand(100, %d);", batch))
	buf.WriteString(fmt.Sprintf("tmp[`vol3] = rand(100, %d);", batch))
	buf.WriteString(fmt.Sprintf("tmp[`vol4] = rand(100, %d);", batch))
	buf.WriteString(fmt.Sprintf("tmp[`vol5] = rand(100, %d);", batch))
	buf.WriteString(fmt.Sprintf("%s.append!(tmp);", tableName))

	_, err := db.RunScript(buf.String())
	util.AssertNil(err)
}

// GoroutineClient checks whether the GoroutineClient is valid
func GoroutineClient(db api.DolphinDB) {
	var err error
	streamConn, err = dialer.NewSimpleConn(context.TODO(), apis.TestAddr, apis.User, apis.Password)
	util.AssertNil(err)

	prepareStreamTable(db, "pub")
	prepareStreamTable(db, "sub1")
	writeStreamTable(db, "pub", 1000)
	client := streaming.NewGoroutineClient("localhost", -1)
	req := &streaming.SubscribeRequest{
		Address:    apis.TestAddr,
		TableName:  "pub",
		ActionName: "action1",
		Handler:    new(sampleHandler1),
		Offset:     0,
		Reconnect:  true,
	}

	client.UnSubscribe(req)

	err = client.Subscribe(req)
	util.AssertNil(err)

	time.Sleep(1 * time.Second)

	df, err := db.RunScript("exec count(*) from sub1")
	util.AssertNil(err)
	util.AssertEqual(df.String(), "int(1000)")

	err = client.UnSubscribe(req)
	util.AssertNil(err)

	client.Close()
	_, _ = db.RunScript("undef(`pub, SHARED)")
	_, _ = db.RunScript("undef(`sub1, SHARED)")

	streamConn.Close()

	logging.Info("example.streaming", "run goroutine client successful")
}

// PollingClient checks whether the PollingClient is valid
func PollingClient(db api.DolphinDB) {
	var err error
	streamConn, err = dialer.NewSimpleConn(context.TODO(), apis.TestAddr, apis.User, apis.Password)
	util.AssertNil(err)

	prepareStreamTable(db, "pub1")
	writeStreamTable(db, "pub1", 1000)
	client := streaming.NewPollingClient("localhost", 8101)
	req := &streaming.SubscribeRequest{
		Address:    apis.TestAddr,
		TableName:  "pub1",
		ActionName: "action1",
		Offset:     0,
		Reconnect:  true,
	}

	poll, err := client.Subscribe(req)
	util.AssertNil(err)

	time.Sleep(1 * time.Second)

	msg := poll.Poll(1000, 1000)
	util.AssertEqual(len(msg), 1000)

	err = client.UnSubscribe(req)
	util.AssertNil(err)

	client.Close()
	_, err = db.RunScript("undef(`pub1, SHARED)")
	util.AssertNil(err)

	streamConn.Close()

	logging.Info("example.streaming", "run polling client successful")
}

// GoroutinePooledClient checks whether the GoroutinePooledClient is valid
func GoroutinePooledClient(db api.DolphinDB) {
	var err error
	streamConn, err = dialer.NewSimpleConn(context.TODO(), apis.TestAddr, apis.User, apis.Password)
	util.AssertNil(err)

	prepareStreamTable(db, "pub2")
	prepareStreamTable(db, "sub2")
	writeStreamTable(db, "pub2", 1000)
	client := streaming.NewGoroutinePooledClient("localhost", 8102)
	req := &streaming.SubscribeRequest{
		Address:    apis.TestAddr,
		TableName:  "pub2",
		ActionName: "action1",
		Handler:    new(sampleHandler2),
		Offset:     0,
		Reconnect:  true,
	}

	err = client.Subscribe(req)
	util.AssertNil(err)

	writeStreamTable(db, "pub2", 1000)
	time.Sleep(1 * time.Second)

	df, err := db.RunScript("exec count(*) from sub2")
	util.AssertNil(err)
	util.AssertEqual(df.String(), "int(2000)")

	err = client.UnSubscribe(req)
	util.AssertNil(err)

	client.Close()
	_, _ = db.RunScript("undef(`pub2, SHARED)")
	_, _ = db.RunScript("undef(`sub2, SHARED)")

	streamConn.Close()

	logging.Info("example.streaming", "run goroutine pooled client successful")
}
