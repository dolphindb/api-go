package dialer

import (
	"context"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dolphindb/api-go/model"

	"github.com/stretchr/testify/assert"
)

const testAddr = "127.0.0.1:3002"

func TestDialer(t *testing.T) {
	fOpt := new(BehaviorOptions)
	assert.Equal(t, fOpt.GetParallelism(), 2)
	assert.Equal(t, fOpt.GetPriority(), 4)
	assert.Equal(t, fOpt.GetFetchSize(), 0)

	fOpt.SetFetchSize(100).
		SetPriority(2).
		SetParallelism(4)
	assert.Equal(t, fOpt.GetParallelism(), 4)
	assert.Equal(t, fOpt.GetPriority(), 2)
	assert.Equal(t, fOpt.GetFetchSize(), 100)

	_, err := NewConn(context.TODO(), testAddr, nil)
	assert.Nil(t, err)

	c, err := NewSimpleConn(context.TODO(), testAddr, "user", "password")
	assert.Nil(t, err)

	// c.AddInitScript("schema()")
	// assert.Equal(t, c.GetInitScripts(), []string{"schema()"})

	// c.SetInitScripts([]string{"init", "login"})
	// assert.Equal(t, c.GetInitScripts(), []string{"init", "login"})

	c.RefreshTimeout(10 * time.Second)

	err = c.Connect()
	assert.Nil(t, err)
	assert.Equal(t, c.IsClosed(), false)

	f, err := os.Create("test.txt")
	assert.Nil(t, err)

	_, err = f.Write([]byte("login"))
	assert.Nil(t, err)

	err = f.Close()
	assert.Nil(t, err)

	_, err = c.RunFile("./test.txt")
	assert.Nil(t, err)

	err = os.Remove("./test.txt")
	assert.Nil(t, err)

	dt, err := model.NewDataType(model.DtString, "test")
	assert.Nil(t, err)

	s := model.NewScalar(dt)
	_, err = c.RunFunc("typestr", []model.DataForm{s})
	assert.Nil(t, err)

	df, err := c.Upload(map[string]model.DataForm{"scalar": s})
	assert.Nil(t, err)
	assert.Equal(t, c.GetSession(), "20267359")
	assert.Equal(t, df.GetDataForm(), model.DfScalar)
	assert.Equal(t, df.GetDataType(), model.DtString)
	assert.Equal(t, df.String(), "string(OK)")

	address := c.GetLocalAddress()
	assert.True(t, strings.HasPrefix(address, "127.0.0.1"))

	err = c.Close()
	assert.Nil(t, err)
	assert.True(t, c.IsClosed())
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
	res := make([]byte, 0)
	for {
		buf := make([]byte, 512)
		l, err := conn.Read(buf)
		if err != nil {
			continue
		}

		res = append(res, buf[0:l]...)
		if len(res) == 15 || len(res) == 29 || len(res) == 30 || len(res) == 48 ||
			len(res) == 48 || len(res) == 54 || len(res) == 49 {
			_, err = conn.Write([]byte{0x32, 0x30, 0x32, 0x36, 0x37, 0x33, 0x35, 0x39, 0x20, 0x30, 0x20, 0x31, 0x0a, 0x4f, 0x4b, 0x0a})
			if err != nil {
				return
			}

			res = make([]byte, 0)
		} else if len(res) == 42 {
			_, err = conn.Write([]byte{0x32, 0x30, 0x32, 0x36, 0x37, 0x33, 0x35, 0x39, 0x20, 0x31, 0x20, 0x31, 0x0a, 0x4f, 0x4b, 0x0a,
				0x12, 0x00, 0x4f, 0x4b, 0x00})
			if err != nil {
				return
			}

			res = make([]byte, 0)
		}
	}
}

func isExit(exit <-chan bool) bool {
	select {
	case <-exit:
		return true
	default:
		return false
	}
}
