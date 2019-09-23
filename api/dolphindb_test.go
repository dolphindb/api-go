package ddb

import "testing"
//import "fmt"

const(

	hostname = "localhost";
	port = 1621;
	user = "admin";
	pass = "123456";
)

func TestDBConnection(t *testing.T){
	var conn DBConnection;
	conn.Init();
	flag := conn.Connect(hostname,port,user,pass);
  if !flag {t.Error("Connect failed");}
//	c := CreateInt(5);
//	if c.GetInt() != 5{
//		t.Error("1");
//	}

}

func TestDBConnection_Run(t *testing.T){
	var conn DBConnection;
	conn.Init();
	conn.Connect(hostname,port,user,pass);
  x := conn.Run("1+1");

  if x.GetInt()!=2 {t.Error("Run Error");}
  
  p := conn.Run("5 4 8");
  p1 := p.ToVector();
  p3 := p1.ToConstant();
  conn.Upload("v1",p3);
  p2 := conn.Run("v1");
  if !p2.IsVector() {t.Error("Upload Error");}; 
}

func TestConstant_Getalltypes(t *testing.T){
    var x int= 1;
	p := CreateInt(x);
	if p.GetInt()!=x {t.Error("GetInt Error");}
	if p.GetLong()!=int64(x) {t.Error("GetLong Error");}
	if p.GetShort()!=int16(x) {t.Error("GetShort Error");}
	if p.GetFloat()!=float32(x) {t.Error("GetFloat Error");}
	if p.GetDouble()!=float64(x) {t.Error("GetDouble Error");}
	if p.GetString()!="1" {t.Error("GetString Error");}
	if !p.GetBool() {t.Error("GetBool Error");}
	if p.GetType()!= DT_INT {t.Error("GetType Error");}
	if p.GetForm()!= DF_SCALAR {t.Error("GetType Error");}
	

}

func TestConstant_Createalltypes(t *testing.T){
	
	p1 := CreateInt(10);
	if p1.GetInt()!=10 {t.Error("CreateInt Error");}
 
	p2 := CreateLong(10);
	if p2.GetLong()!=10 {t.Error("CreateLong Error");}
 
	p3 := CreateShort(10);
	if p3.GetShort()!=10 {t.Error("CreateShort Error");}
 
	p4 := CreateFloat(10.0);
	if p4.GetFloat()!=10 {t.Error("CreateFloat Error");}
 
	p5 := CreateDouble(10.0);
	if p5.GetDouble()!=10 {t.Error("CreateDouble Error");}

	p6 := CreateBool(true);
	if !p6.GetBool() {t.Error("CreateDouble Error");}

	p7 := CreateString("1231231");
	if p7.GetString()!="1231231" {t.Error("CreateString Error");}
 
}

func TestConstant_IsForm(t *testing.T){
	var conn DBConnection;
	conn.Init();
	conn.Connect(hostname,port,user,pass);
    p1 := conn.Run("1+1");
	if !p1.IsScalar() { t.Error("IsScalar Error"); }
	
	p2 := conn.Run("5 4 8");
	if !p2.IsArray() { t.Error("IsArray Error"); }
	
	p3 := conn.Run("3:5");
	if !p3.IsPair() { t.Error("IsPair Error"); }
	
	p4 := conn.Run("1..6$2:3");
	if !p4.IsMatrix() { t.Error("IsMatrix Error"); }

	p5 := conn.Run("set(3 5 4 6)");
	if !p5.IsSet() { t.Error("IsSet Error"); }

	p6 := conn.Run("dict(`IBM`MS`ORCL, 170.5 56.2 49.5)");
	if !p6.IsDictionary() { t.Error("IsDictionary Error"); }

	p7 := conn.Run("table(`IBM`MS`ORCL as sym, 170.5 56.2 49.5 as price)");
	if !p7.IsTable() { t.Error("IsTable Error"); }

	p8 := conn.Run("(1 2 3, `IBM`MSFT`GOOG, 2.5)");
	if !p8.IsVector() { t.Error("IsVector Error"); }

}

func TestVector(t *testing.T){

   p1 := CreateVector(DT_INT);
   if p1.Size()!= 0  { t.Error("CreateVector Error"); }
 //  t1 :=CreateInt(1);
   p1.Append(CreateInt(1));
   if p1.Size()!= 1 { t.Error("Append Error"); }
   t1 := p1.Get(0)
   if t1.GetInt()!= 1 { t.Error("Append Error"); }


   p1.Append(CreateInt(1));
   p1.Append(CreateInt(1));
   p1.Remove(1);
   if p1.Size()!= 2  { t.Error("Remove Error"); }

   p1.SetName("v1");
   if p1.GetName()!= "v1"  { t.Error("SetName Error"); }

   var conn DBConnection;
   conn.Init();
   conn.Connect(hostname,port,user,pass);
   p2 :=  conn.Run("5 4 8");
   p3 := p2.ToVector();
   if !p3.IsVector() { t.Error("ToVector Error"); }
}

func TestTable(t *testing.T){

	var conn DBConnection;
	conn.Init();
	conn.Connect(hostname,port,user,pass);
	p1 := conn.Run("table(`IBM`MS`ORCL as sym, 170.5 56.2 49.5 as price)");
	if (p1.Size() != 3) { t.Error("RungetTable Error"); }
	p2 := p1.ToTable();
	if !p2.IsTable() {t.Error("ToTable Error"); }
	p3 := p2.GetColumn(1);
	if !p3.IsVector() {t.Error("getColumn Error"); }
	p4 := p2.GetColumnbyName("price");
	if !p4.IsVector() {t.Error("getColumnbyName Error"); }
	p5 := p2.Columns();
	if p5 != 2 {t.Error("getColumns Error"); }

	v1 := CreateVector(DT_INT);
	v2 := CreateVector(DT_INT)
	cols := [] Vector {v1,v2};
	v1.Append(CreateInt(1));
	v2.Append(CreateInt(1));
	colnames := [] string {"v1","v2"};
	ta := CreateTable(colnames, cols);
	if ta.Size()!=1 {t.Error("CreateTable Error"); }
}

func TestgetSlice(t *testing.T){
   
	var conn DBConnection;
	conn.Init();
	conn.Connect(hostname,port,user,pass);
	px := conn.Run("1 2 3 4 5 6 7 8 9 0");
    p := px.ToVector();
	p1 := p.GetIntSlice();
	if (len(p1)!= 10) { t.Error("GetIntSlice Error"); }
	p2 := p.GetShortSlice();
	if (len(p2)!= 10) { t.Error("GetShortSlice Error"); }
	p3 := p.GetLongSlice();
	if (len(p3)!= 10) { t.Error("GetLongSlice Error"); }
	p4 := p.GetBoolSlice();
	if (len(p4)!= 10) { t.Error("GetBoolSlice Error"); }
	p5 := p.GetFloatSlice();
	if (len(p5)!= 10) { t.Error("GetFloatSlice Error"); }
	p6 := p.GetDoubleSlice();
	if (len(p6)!= 10) { t.Error("GetDoubleSlice Error"); }
	p7 := p.GetStringSlice();
	if (len(p7)!= 10) { t.Error("GetStringSlice Error"); }
//	p1 := p.GetIntSlice();
//    if (len(p1)!= 10) { t.Error("GetIntSlice Error"); }
}