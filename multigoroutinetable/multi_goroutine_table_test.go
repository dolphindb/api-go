package multigoroutinetable

import (
	"bytes"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dolphindb/api-go/v3/dialer/protocol"
	"github.com/dolphindb/api-go/v3/model"
	"github.com/stretchr/testify/assert"
)

const testAddr = "127.0.0.1:3001"

func TestMultiGoroutineTable(t *testing.T) {
	opt := &Option{
		GoroutineCount: 2,
		BatchSize:      1,
		Throttle:       1,
		PartitionCol:   "sym",
		Database:       "dfs://db",
		TableName:      "tb",
		UserID:         "user",
		Password:       "password",
		Address:        testAddr,
	}
	mtt, err := NewMultiGoroutineTable(opt)
	assert.Nil(t, err)

	err = mtt.Insert(time.Date(1970, 1, 1, 1, 1, 1, 1, time.UTC), "insert")
	assert.Nil(t, err)
	err = mtt.Insert(time.Date(1970, 1, 1, 1, 1, 1, 1, time.UTC), "success")
	assert.Nil(t, err)

	df := mtt.GetUnwrittenData()
	assert.Equal(t, len(df), 1)

	// date, err := model.NewDataType(model.DtDate, time.Date(1970, 1, 1, 1, 1, 1, 1, time.UTC))
	// assert.Nil(t, err)

	// sym, err := model.NewDataType(model.DtString, "insertFailed")
	// assert.Nil(t, err)

	// err = mtt.InsertUnwrittenData([][]model.DataType{
	// 	{
	// 		date, sym,
	// 	},
	// })
	// assert.Nil(t, err)

	mtt.WaitForGoroutineCompletion()

	sts := mtt.GetStatus()
	assert.Equal(t, sts.IsExit, true)

	opt.TableName = "db"
	opt.Database = ""
	mtt, err = NewMultiGoroutineTable(opt)
	assert.Nil(t, err)
	err = mtt.Insert(time.Date(1970, 1, 1, 1, 1, 1, 1, time.UTC), "insert")
	assert.Nil(t, err)

	err = mtt.Insert(time.Date(1970, 1, 1, 1, 1, 1, 1, time.UTC), "tested")
	assert.Nil(t, err)

	err = mtt.Insert(time.Date(1970, 1, 1, 1, 1, 1, 1, time.UTC), "sample")
	assert.Nil(t, err)

	err = mtt.Insert(time.Date(1970, 1, 1, 1, 1, 1, 1, time.UTC), "success")
	assert.Nil(t, err)

	df = mtt.GetUnwrittenData()
	err = mtt.InsertUnwrittenData(df)
	assert.Nil(t, err)

	mtt.WaitForGoroutineCompletion()
	sts = mtt.GetStatus()
	assert.Equal(t, sts.String(), "errMsg         :  \nisExit         :  true\nsentRows       :  4\nunsentRows     :  0\nsendFailedRows :  0\ngoroutineStatus   :\n    goroutineIndex: 0, sentRows: 1, unsentRows: 0, sendFailedRows: 0\n    goroutineIndex: 1, sentRows: 3, unsentRows: 0, sendFailedRows: 0\n")

	opt.GoroutineCount = 1
	opt.Database = "dbScalar"
	_, err = NewMultiGoroutineTable(opt)
	assert.Nil(t, err)
}

func TestMain(m *testing.M) {
	exit := make(chan bool)
	ln, err := net.Listen("tcp", testAddr)
	if err != nil {
		return
	}

	go func() {
		for !isExit(exit) {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			go handleData(conn)
		}

		ln.Close()
	}()

	exitCode := m.Run()

	close(exit)

	os.Exit(exitCode)
}

func handleData(conn net.Conn) {
	const (
		boolScalarTrueResponse = "20267359 1 1\nOK\n\x01\x00\x01"
		successResponse        = "20267359 0 1\nOK\n"
		malformedInsertReply   = "20267359 0\nOK\n"
		scramUnavailableError  = "20267359 0 1\nCan't recognize function name scramClientFirst\n"
	)

	res := make([]byte, 0)
	for {
		buf := make([]byte, 512)
		l, err := conn.Read(buf)
		if err != nil {
			continue
		}

		res = append(res, buf[0:l]...)
		script := string(res)
		var resp []byte

		if strings.Contains(script, "scramClientFirst") {
			resp = []byte(scramUnavailableError)
		} else if strings.Contains(script, "isNodeInitialized") {
			resp = []byte(boolScalarTrueResponse)
		} else if strings.Contains(script, "login") || strings.Contains(script, "connect") {
			resp = []byte(successResponse)
		} else if strings.Contains(script, `schema(loadTable("dfs://db","tb"))`) {
			resp = buildPartitionedSchemaResponse()
		} else if strings.Contains(script, "schema(db)") || strings.Contains(script, `schema(loadTable("dbScalar","db"))`) {
			resp = buildNonPartitionSchemaResponse()
		} else if strings.Contains(script, "tableInsert") {
			if strings.Contains(script, `loadTable("dfs://db","tb")`) && strings.Contains(script, "insert") && !strings.Contains(script, "success") {
				time.Sleep(20 * time.Millisecond)
				resp = []byte(malformedInsertReply)
			} else {
				resp = []byte(successResponse)
			}
		}

		if len(resp) > 0 {
			_, err = conn.Write(resp)
			if err != nil {
				return
			}

			res = make([]byte, 0)
		}
	}
}

func buildPartitionedSchemaResponse() []byte {
	colDefs := mustTable(
		[]string{"name", "typeInt"},
		[]model.DataTypeByte{model.DtString, model.DtString},
		[]interface{}{
			[]string{"date", "sym"},
			[]string{
				strconv.Itoa(int(model.DtDate)),
				strconv.Itoa(int(model.DtString)),
			},
		},
	)

	keys := mustVector(model.DtString, []string{
		"partitionColumnName",
		"partitionColumnIndex",
		"partitionSchema",
		"partitionType",
		"colDefs",
	})
	values := mustVector(model.DtAny, []model.DataForm{
		mustScalar(model.DtString, "sym"),
		mustScalar(model.DtInt, int32(1)),
		mustVector(model.DtString, []string{"insert", "success"}),
		mustScalar(model.DtInt, int32(1)),
		colDefs,
	})

	return renderResponse(model.NewDictionary(keys, values))
}

func buildNonPartitionSchemaResponse() []byte {
	colDefs := mustTable(
		[]string{"name", "typeInt"},
		[]model.DataTypeByte{model.DtString, model.DtString},
		[]interface{}{
			[]string{"date", "sym"},
			[]string{
				strconv.Itoa(int(model.DtDate)),
				strconv.Itoa(int(model.DtString)),
			},
		},
	)

	keys := mustVector(model.DtString, []string{"colDefs"})
	values := mustVector(model.DtAny, []model.DataForm{colDefs})

	return renderResponse(model.NewDictionary(keys, values))
}

func mustScalar(dt model.DataTypeByte, value interface{}) *model.Scalar {
	data, err := model.NewDataType(dt, value)
	if err != nil {
		panic(err)
	}

	return model.NewScalar(data)
}

func mustVector(dt model.DataTypeByte, value interface{}) *model.Vector {
	data, err := model.NewDataTypeListFromRawData(dt, value)
	if err != nil {
		panic(err)
	}

	return model.NewVector(data)
}

func mustTable(colNames []string, colTypes []model.DataTypeByte, colValues []interface{}) *model.Table {
	table, err := model.NewTableFromRawData(colNames, colTypes, colValues)
	if err != nil {
		panic(err)
	}

	return table
}

func renderResponse(df model.DataForm) []byte {
	buf := bytes.NewBufferString("20267359 1 1\nOK\n")
	w := protocol.NewWriter(buf)
	if err := df.Render(w, protocol.LittleEndian); err != nil {
		panic(err)
	}
	if err := w.Flush(); err != nil {
		panic(err)
	}

	return buf.Bytes()
}

func isExit(exit <-chan bool) bool {
	select {
	case <-exit:
		return true
	default:
		return false
	}
}
