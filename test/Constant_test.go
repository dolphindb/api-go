package test

import (
	"../src"
	"testing"
)

func TestConstant_GetInt(t *testing.T) {
	var x int = 1
	p := ddb.CreateInt(x)
	if p.GetInt() != x {
		t.Error("GetInt Error")
	}
	ddb.DelConstant(p)
}

func TestConstant_GetLong(t *testing.T) {
	var x int = 1
	p := ddb.CreateInt(x)
	if p.GetLong() != int64(x) {
		t.Error("GetLong Error")
	}
	ddb.DelConstant(p)
}

func TestConstant_GetShort(t *testing.T) {
	var x int = 1
	p := ddb.CreateInt(x)
	if p.GetShort() != int16(x) {
		t.Error("GetShort Error")
	}
	ddb.DelConstant(p)
}

func TestConstant_GetFloat(t *testing.T) {
	var x int = 1
	p := ddb.CreateInt(x)
	if p.GetFloat() != float32(x) {
		t.Error("GetFloat Error")
	}
	ddb.DelConstant(p)
}

func TestConstant_GetDouble(t *testing.T) {
	var x int = 1
	p := ddb.CreateInt(x)
	if p.GetDouble() != float64(x) {
		t.Error("GetDouble Error")
	}
	ddb.DelConstant(p)
}

func TestConstant_GetString(t *testing.T) {
	var x int = 1
	p := ddb.CreateInt(x)
	if p.GetString() != "1" {
		t.Error("GetString Error")
	}
	ddb.DelConstant(p)
}

func TestConstant_GetBool(t *testing.T) {
	var x int = 1
	p := ddb.CreateInt(x)
	if !p.GetBool() {
		t.Error("GetBool Error")
	}
	ddb.DelConstant(p)
}

func TestConstant_GetType(t *testing.T) {
	var x int = 1
	p := ddb.CreateInt(x)
	if p.GetType() != ddb.DT_INT {
		t.Error("GetType Error")
	}
	ddb.DelConstant(p)
}

func TestConstant_GetForm(t *testing.T) {
	var x int = 1
	p := ddb.CreateInt(x)
	if p.GetForm() != ddb.DF_SCALAR {
		t.Error("GetType Error")
	}
	ddb.DelConstant(p)
}

func TestConstant_CreateInt(t *testing.T) {

	p1 := ddb.CreateInt(10)
	if p1.GetInt() != 10 {
		t.Error("CreateInt Error")
	}
	ddb.DelConstant(p1)
}

func TestConstant_CreatLong(t *testing.T) {
	p2 := ddb.CreateLong(10)
	if p2.GetLong() != 10 {
		t.Error("CreateLong Error")
	}
	ddb.DelConstant(p2)
}

func TestConstant_CreateallShort(t *testing.T) {
	p3 := ddb.CreateShort(10)
	if p3.GetShort() != 10 {
		t.Error("CreateShort Error")
	}
	ddb.DelConstant(p3)

}

func TestConstant_CreateFloat(t *testing.T) {
	p4 := ddb.CreateFloat(10.0)
	if p4.GetFloat() != 10 {
		t.Error("CreateFloat Error")
	}
	ddb.DelConstant(p4)

}

func TestConstant_CreateDouble(t *testing.T) {
	p5 := ddb.CreateDouble(10.0)
	if p5.GetDouble() != 10 {
		t.Error("CreateDouble Error")
	}
	ddb.DelConstant(p5)

}

func TestConstant_CreateBool(t *testing.T) {

	p6 := ddb.CreateBool(true)
	if !p6.GetBool() {
		t.Error("CreateDouble Error")
	}
	ddb.DelConstant(p6)

}

func TestConstant_CreateString(t *testing.T) {
	p7 := ddb.CreateString("1231231")
	if p7.GetString() != "1231231" {
		t.Error("CreateString Error")
	}
	ddb.DelConstant(p7)

}
