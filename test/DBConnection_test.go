package test

import (
	"../src"
	"testing"
)

const (
	hostname = "127.0.0.1"
	port     = 1621
	user     = "admin"
	pass     = "123456"
)

func TestDBConnection(t *testing.T) {
	var conn ddb.DBConnection
	conn.Init()
	flag := conn.Connect(hostname, port, user, pass)
	if !flag {
		t.Error("Connect failed")
	}
	conn.Close()
}

func TestDBConnection_Run(t *testing.T) {
	var conn ddb.DBConnection
	conn.Init()
	conn.Connect(hostname, port, user, pass)
	x := conn.Run("1+1")

	if x.GetInt() != 2 {
		t.Error("Run Error")
	}
	conn.Close()
}

func TestDBConnection_Upload(t *testing.T) {
	var conn ddb.DBConnection
	conn.Init()
	conn.Connect(hostname, port, user, pass)

	p := conn.Run("5 4 8")
	p1 := p.ToVector()
	p3 := p1.ToConstant()
	conn.Upload("v1", p3)
	p2 := conn.Run("v1")
	if !p2.IsVector() {
		t.Error("Upload Error")
	}
	conn.Close()
}

func TestDBConnection_Runfunc(t *testing.T) {
	var conn ddb.DBConnection
	conn.Init()
	conn.Connect(hostname, port, user, pass)
	conn.Run("x = [1,3,5]")
	a2 := []int32{9, 8, 7}
	y0 := ddb.CreateVector(ddb.DT_INT, 3)
	y0.SetIntArray(0, 3, a2)
	y := y0.ToConstant()
	args := []ddb.Constant{y}
	result1 := conn.RunFunc("add{x,}", args)

	if !result1.IsVector() {
		t.Error("Runfunc Error")
	}
	conn.Close()
}
