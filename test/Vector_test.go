package test

import (
	"../src"
	"testing"
)

/*
const(
	hostname = "127.0.0.1";
	port = 8848;
	user = "admin";
	pass = "123456";
)
*/

func TestVector_CreateVector(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_INT, 0)
	if !p1.IsVector() {
		t.Error("CreateVector Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_Append(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_INT, 0)
	p1.Append(ddb.CreateInt(1))
	if p1.Size() != 1 {
		t.Error("Append Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_Remove(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_INT, 0)
	p1.Append(ddb.CreateInt(1))
	p1.Append(ddb.CreateInt(1))
	p1.Append(ddb.CreateInt(1))
	p1.Remove(1)
	if p1.Size() != 2 {
		t.Error("Remove Error")
	}

	ddb.DelConstant(p1.ToConstant())
}

func TestVector_SetName(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_INT, 0)
	p1.SetName("v1")
	if p1.GetName() != "v1" {
		t.Error("SetName Error")
	}

	ddb.DelConstant(p1.ToConstant())
}

func TestVector_AppendInt(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_INT, 0)
	s := []int32{1, 2, 3, 4, 5, 6}
	p1.AppendInt(s, 6)
	if p1.Size() != 6 {
		t.Error("AppendInt Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_AppendShort(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_SHORT, 0)
	s := []int16{1, 2, 3, 4, 5, 6}
	p1.AppendShort(s, 6)
	if p1.Size() != 6 {
		t.Error("AppendShort Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_AppendLong(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_LONG, 0)
	s := []int64{1, 2, 3, 4, 5, 6}
	p1.AppendLong(s, 6)
	if p1.Size() != 6 {
		t.Error("AppendLong Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_AppendFloat(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_FLOAT, 0)
	s := []float32{1, 2, 3, 4, 5, 6}
	p1.AppendFloat(s, 6)
	if p1.Size() != 6 {
		t.Error("AppendFloat Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_AppendDouble(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_DOUBLE, 0)
	s := []float64{1, 2, 3, 4, 5, 6}
	p1.AppendDouble(s, 6)
	if p1.Size() != 6 {
		t.Error("AppendDouble Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_AppendBool(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_BOOL, 0)
	s := []bool{true, false, false, true, false, true}
	p1.AppendBool(s, 6)
	if p1.Size() != 6 {
		t.Error("AppendBool Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_AppendString(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_STRING, 0)
	s := []string{"one", "one", "one", "one", "one", "one"}
	p1.AppendString(s, 6)
	if p1.Size() != 6 {
		t.Error("AppendString Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_SetIntArray(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_INT, 0)
	s := []int32{1, 2, 3, 4, 5, 6}
	p1.AppendInt(s, 6)
	a1 := []int32{0, 0, 0, 0}
	p1.SetIntArray(0, 4, a1)
	x := p1.Get(2)
	if x.GetInt() != 0 {
		t.Error("SetIntArray Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_SetShortArray(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_SHORT, 0)
	s := []int16{1, 2, 3, 4, 5, 6}
	p1.AppendShort(s, 6)
	a1 := []int16{0, 0, 0, 0}
	p1.SetShortArray(0, 4, a1)
	x := p1.Get(2)
	if x.GetShort() != 0 {
		t.Error("SetShortArray Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_SetLongArray(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_LONG, 0)
	s := []int64{1, 2, 3, 4, 5, 6}
	p1.AppendLong(s, 6)
	a1 := []int64{0, 0, 0, 0}
	p1.SetLongArray(0, 4, a1)
	x := p1.Get(2)
	if x.GetLong() != 0 {
		t.Error("SetLongArray Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_SetFloatArray(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_FLOAT, 0)
	s := []float32{1, 2, 3, 4, 5, 6}
	p1.AppendFloat(s, 6)
	a1 := []float32{0, 0, 0, 0}
	p1.SetFloatArray(0, 4, a1)
	x := p1.Get(2)
	if x.GetFloat() != 0 {
		t.Error("SetFloatArray Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_SetDoubleArray(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_DOUBLE, 0)
	s := []float64{1, 2, 3, 4, 5, 6}
	p1.AppendDouble(s, 6)
	a1 := []float64{0, 0, 0, 0}
	p1.SetDoubleArray(0, 4, a1)
	x := p1.Get(2)
	if x.GetDouble() != 0 {
		t.Error("SetDoubleArray Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_SetBoolArray(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_BOOL, 0)
	s := []bool{true, true, true, true, true, true}
	p1.AppendBool(s, 6)
	arr1 := []bool{false, false, false, false}
	p1.SetBoolArray(0, 4, arr1)
	x := p1.Get(2)
	if x.GetBool() {
		t.Error("AppendBool Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_SetStringArray(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_STRING, 0)
	s := []string{"one", "one", "one", "one", "one", "one"}
	p1.AppendString(s, 6)
	arr1 := []string{"two", "two", "two", "two"}
	p1.SetStringArray(0, 4, arr1)
	x := p1.Get(2)
	if x.GetString() != "two" {
		t.Error("AppendBool Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_SetIntByIndex(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_INT, 0)
	s := []int32{1, 2, 3, 4, 5, 6}
	p1.AppendInt(s, 6)
	p1.SetIntByIndex(2, 0)
	x := p1.Get(2)
	if x.GetInt() != 0 {
		t.Error("SetIntByIndex Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_SetShortByIndex(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_SHORT, 0)
	s := []int16{1, 2, 3, 4, 5, 6}
	p1.AppendShort(s, 6)
	p1.SetShortByIndex(2, 0)
	x := p1.Get(2)
	if x.GetShort() != 0 {
		t.Error("SetShortByIndex Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_SetLongByIndex(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_LONG, 0)
	s := []int64{1, 2, 3, 4, 5, 6}
	p1.AppendLong(s, 6)
	p1.SetLongByIndex(2, 0)
	x := p1.Get(2)
	if x.GetLong() != 0 {
		t.Error("SetLongByIndex Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_SetFloatByIndex(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_FLOAT, 0)
	s := []float32{1, 2, 3, 4, 5, 6}
	p1.AppendFloat(s, 6)
	p1.SetFloatByIndex(2, 0)
	x := p1.Get(2)
	if x.GetFloat() != 0 {
		t.Error("SetFloatByIndex Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_SetDoubleByIndex(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_DOUBLE, 0)
	s := []float64{1, 2, 3, 4, 5, 6}
	p1.AppendDouble(s, 6)
	p1.SetDoubleByIndex(2, 0)
	x := p1.Get(2)
	if x.GetDouble() != 0 {
		t.Error("SetFloatByIndex Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_SetBoolByIndex(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_BOOL, 0)
	s := []bool{true, true, true, true, true, true}
	p1.AppendBool(s, 6)
	p1.SetBoolByIndex(2, false)
	x := p1.Get(2)
	if x.GetBool() {
		t.Error("AppendBool Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_SetStringByIndex(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_STRING, 0)
	s := []string{"one", "one", "one", "one", "one", "one"}
	p1.AppendString(s, 6)
	p1.SetStringByIndex(2, "two")
	x := p1.Get(2)
	if x.GetString() != "two" {
		t.Error("AppendBool Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_GetCapacity(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_STRING, 0)
	s := []string{"one", "one", "one", "one", "one", "one"}
	p1.AppendString(s, 6)
	if p1.GetCapacity() != 6 {
		t.Error("GetCapacity Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_Reserve(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_STRING, 0)
	s := []string{"one", "one", "one", "one", "one", "one"}
	p1.AppendString(s, 6)
	p1.Reserve(10)
	if p1.GetCapacity() != 10 {
		t.Error("Reserve Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_GetUnitLength(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_INT, 0)
	s := []int32{1, 2, 3, 4, 5, 6}
	p1.AppendInt(s, 6)
	if p1.GetUnitLength() != 4 {
		t.Error("AppendInt Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_Clear(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_INT, 0)
	s := []int32{1, 2, 3, 4, 5, 6}
	p1.AppendInt(s, 6)
	p1.Clear()
	if p1.Size() != 0 {
		t.Error("AppendInt Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_GetSubVector(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_INT, 0)
	s := []int32{1, 2, 3, 4, 5, 6}
	p1.AppendInt(s, 6)
	p2 := p1.GetSubVector(1, 3)
	x := p2.Get(1)
	if x.GetInt() != 3 {
		t.Error("GetSubVector Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_Fill(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_INT, 0)
	s := []int32{1, 2, 3, 4, 5, 6}
	p1.AppendInt(s, 6)
	p1.Fill(1, 4, ddb.CreateInt(0))
	x := p1.Get(3)
	if x.GetInt() != 0 {
		t.Error("Fill Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_Reverse(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_INT, 0)
	s := []int32{1, 2, 3, 4, 5, 6}
	p1.AppendInt(s, 6)
	p1.Reverse()
	x := p1.Get(0)
	if x.GetInt() != 6 {
		t.Error("Reverse Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

func TestVector_Replace(t *testing.T) {
	p1 := ddb.CreateVector(ddb.DT_INT, 0)
	s := []int32{1, 2, 3, 4, 5, 6}
	p1.AppendInt(s, 6)
	p1.Replace(ddb.CreateInt(5), ddb.CreateInt(10))
	x := p1.Get(4)
	if x.GetInt() != 10 {
		t.Error("Replace Error")
	}
	ddb.DelConstant(p1.ToConstant())
}

/*
int Vector_getCapacity(Constant* w);
int Vector_reserve(Constant* w, int x);
int Vector_appendInt(Constant* v, int* x, int len);
int Vector_appendShort(Constant* v, short * x, int len);
int Vector_appendLong(Constant* v, long long* x, int len);
int Vector_appendFloat(Constant* v, float* x, int len);
int Vector_appendDouble(Constant* v, double* x, int len);
int Vector_appendString(Constant* v, char* x, int len);
int Vector_appendBool(Constant* v, char* x, int len);
*/
