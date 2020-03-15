package test

import (
	"../src"
	"testing"
)

func Test_ToTable(t *testing.T) {

	var conn ddb.DBConnection
	conn.Init()
	conn.Connect(hostname, port, user, pass)
	p1 := conn.Run("table(`IBM`MS`ORCL as sym, 170.5 56.2 49.5 as price)")
	if p1.Size() != 3 {
		t.Error("RungetTable Error")
	}
	p2 := p1.ToTable()
	if !p2.IsTable() {
		t.Error("ToTable Error")
	}
}

func Test_GetColumn(t *testing.T) {
	var conn ddb.DBConnection
	conn.Init()
	conn.Connect(hostname, port, user, pass)
	p1 := conn.Run("table(`IBM`MS`ORCL as sym, 170.5 56.2 49.5 as price)")
	p2 := p1.ToTable()
	p3 := p2.GetColumn(1)
	if !p3.IsVector() {
		t.Error("getColumn Error")
	}

}

func Test_GetColumnbyName(t *testing.T) {
	var conn ddb.DBConnection
	conn.Init()
	conn.Connect(hostname, port, user, pass)
	p1 := conn.Run("table(`IBM`MS`ORCL as sym, 170.5 56.2 49.5 as price)")
	p2 := p1.ToTable()
	p4 := p2.GetColumnByName("price")
	if !p4.IsVector() {
		t.Error("getColumnbyName Error")
	}
}

func Test_Columns(t *testing.T) {
	var conn ddb.DBConnection
	conn.Init()
	conn.Connect(hostname, port, user, pass)
	p1 := conn.Run("table(`IBM`MS`ORCL as sym, 170.5 56.2 49.5 as price)")
	p2 := p1.ToTable()
	p5 := p2.Columns()
	if p5 != 2 {
		t.Error("getColumns Error")
	}
}

func Test_SetColumnName(t *testing.T) {
	var conn ddb.DBConnection
	conn.Init()
	conn.Connect(hostname, port, user, pass)
	p1 := conn.Run("table(`IBM`MS`ORCL as sym, 170.5 56.2 49.5 as price)")
	p2 := p1.ToTable()
	p2.SetColumnName(0, "sss")
	x := p2.GetColumnName(0)
	if x != "sss" {
		t.Error("SetColumnName  Error")
	}
}

func Test_GetColumnIndex(t *testing.T) {
	var conn ddb.DBConnection
	conn.Init()
	conn.Connect(hostname, port, user, pass)
	p1 := conn.Run("table(`IBM`MS`ORCL as sym, 170.5 56.2 49.5 as price)")
	p2 := p1.ToTable()
	x := p2.GetColumnIndex("sym")
	if x != 0 {
		t.Error("GetColumnIndex Error")
	}
}

func Test_Contain(t *testing.T) {
	var conn ddb.DBConnection
	conn.Init()
	conn.Connect(hostname, port, user, pass)
	p1 := conn.Run("table(`IBM`MS`ORCL as sym, 170.5 56.2 49.5 as price)")
	p2 := p1.ToTable()
	x := p2.Contain("sym")
	if x != true {
		t.Error("Contain Error")
	}
}

func Test_GetStringByIndex(t *testing.T) {
	var conn ddb.DBConnection
	conn.Init()
	conn.Connect(hostname, port, user, pass)
	p1 := conn.Run("table(`IBM`MS`ORCL as sym, 170.5 56.2 49.5 as price)")
	p2 := p1.ToTable()
	x := p2.GetStringByIndex(0)
	if x == "" {
		t.Error("GetStringByIndex Error")
	}
}
