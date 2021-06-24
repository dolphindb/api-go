package test

import (
	"../src"
	"testing"
)

const (
	hostname = "127.0.0.1"
	port     = 28848
	user     = "admin"
	pass     = "123456"
)

func TestSet_ToSet(t *testing.T) {

	var conn ddb.DBConnection
	conn.Init()
	conn.Connect(hostname, port, user, pass)
	p1 := conn.Run("set(4 6 7)")
	s := p1.ToSet()

	if !s.IsSet() {
		t.Error("ToSet Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestSet_Clear(t *testing.T) {

	var conn ddb.DBConnection
	conn.Init()
	conn.Connect(hostname, port, user, pass)
	p1 := conn.Run("set(4 6 7)")
	s := p1.ToSet()
	s.Clear()
	if s.Size() != 0 {
		t.Error("Clear Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestSet_Remove(t *testing.T) {

	var conn ddb.DBConnection
	conn.Init()
	conn.Connect(hostname, port, user, pass)
	p1 := conn.Run("set(4 6 7)")
	s := p1.ToSet()
	s.Remove(ddb.CreateInt(7))
	if s.Size() != 2 {
		t.Error("Remove Error")
	}
	ddb.DelConstant(p1.ToConstant())
}
func TestSet_Append(t *testing.T) {

	var conn ddb.DBConnection
	conn.Init()
	conn.Connect(hostname, port, user, pass)
	p1 := conn.Run("set(4 6 7)")
	s := p1.ToSet()
	s.Append(ddb.CreateInt(9))
	if s.Size() != 4 {
		t.Error("Remove Error")
	}
	ddb.DelConstant(p1.ToConstant())
}
