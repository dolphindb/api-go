package ddb

/*
typedef struct DBConnection DBConnection;
typedef struct Constant Constant;

DBConnection* DBConnection_new();
int DBConnection_connect(DBConnection* conn, char* host, int port, char* user, char* pass);
Constant* DBConnection_run(DBConnection* conn, char* s);
void DBConnection_close(DBConnection* conn);
void DBConnection_upload(DBConnection* conn, char* name, Constant* w);
Constant* DBConnection_runfunc(DBConnection* conn, char* script, Constant* args);

int Constant_getForm(Constant* w);
int Constant_getType(Constant* w);

int Constant_getBool(Constant* w);
int Constant_getInt(Constant* w);
short Constant_getShort(Constant* w);
long long Constant_getLong(Constant* w);
float Constant_getFloat(Constant* w);
double Constant_getDouble(Constant* w);
char* Constant_getString(Constant* w);

int Constant_isScalar(Constant* w);
int Constant_isArray(Constant* w);
int Constant_isPair(Constant* w);
int Constant_isMatrix(Constant* w);
int Constant_isVector(Constant* w);
int Constant_isTable(Constant* w);
int Constant_isSet(Constant* w);
int Constant_isDictionary(Constant* w);

int Constant_size(Constant* w);

Constant*  toConstant(Constant* w);

Constant*  toVector(Constant* w);
void Vector_setName(Constant* w, char* vname);
char* Vector_getName(Constant* w);
Constant* Vector_get(Constant* w, int x);
int Vector_remove(Constant* v,int x);
int Vector_append(Constant* v,Constant* w);

Constant*  toTable(Constant* w);
void Table_setName(Constant* w, char* tname);
char* Table_getName(Constant* w);
Constant* Table_getColumn(Constant* w,int x);
char* Table_getColumnName(Constant* w,int x);
int Table_columns(Constant* w);
int Table_rows(Constant* w);
int Table_getColumnType(Constant* w,int x);
Constant* Table_getColumnbyName(Constant* w,char* s);

Constant* createInt(int val);
Constant* createBool(int val);
Constant* createShort(short val);
Constant* createLong(long long val);
Constant* createFloat(float val);
Constant* createDouble(double val);
Constant* createString(char* val);

Constant* createDate(int year, int month, int day);
Constant* createMonth(int year, int month);
Constant* createNanoTime(int hour, int minute, int second, int nanosecond);
Constant* createTime(int hour, int minute, int second, int millisecond);
Constant* createSecond(int hour, int minute, int second);
Constant* createMinute(int hour, int minute);
Constant* createNanoTimestamp(int year, int month, int day, int hour, int minute, int second, int nanosecond);
Constant* createTimestamp(int year, int month, int day, int hour, int minute, int second, int millisecond);
Constant* createDateTime(int year, int month, int day, int hour, int minute, int second);

Constant* createVector(int type);
Constant* createTable(Constant* colname,Constant* cols,int len);
#cgo LDFLAGS: -L./api-c -lwrapper -Wl,-rpath,./api/api-c
*/
import "C"
//import "unsafe"
//import "fmt"

const ( DT_VOID = iota
		DT_BOOL
		DT_CHAR
		DT_SHORT
		DT_INT
		DT_LONG
		DT_DATE
		DT_MONTH
		DT_TIME
		DT_MINUTE
		DT_SECOND
		DT_DATETIME
		DT_TIMESTAMP
		DT_NANOTIME
		DT_NANOTIMESTAMP
		DT_FLOAT
		DT_DOUBLE
		DT_SYMBOL
		DT_STRING
		DT_UUID
		DT_FUNCTIONDEF
		DT_HANDLE
		DT_CODE
		DT_DATASOURCE
		DT_RESOURCE
		DT_ANY
		DT_COMPRESS
		DT_DICTIONARY
		DT_OBJECT);
const (
	DF_SCALAR = iota
	DF_VECTOR
	DF_PAIR
	DF_MATRIX
	DF_SET
	DF_DICTIONARY
	DF_TABLE
	DF_CHART
	DF_CHUNK
);

func tobool(x C.int) bool{
	if int(x) != 0 {return true};
	return false;
}

type DBConnection struct {
	ptr *C.DBConnection
}

type Constant struct {
	ptr *C.Constant
}

type Vector struct {
	Constant;
}

type Table struct {
	Constant;
}


func CreateInt(x int) Constant{
   
	return Constant{C.createInt(C.int(x))};
	 
}

func CreateBool(x bool) Constant{
	var y int;
	if x == true { y = 1
	} else { y = 0 
	};
	return Constant{C.createBool(C.int(y))};
	 
}

func CreateShort(x int16) Constant{
   
	return Constant{C.createShort(C.short(x))};
	 
}

func CreateLong(x int64) Constant{
   
	return Constant{C.createLong(C.longlong(x))};
	 
}

func CreateFloat(x float32) Constant{
   
	return Constant{C.createFloat(C.float(x))};
	 
}
func CreateDouble(x float64) Constant{
   
	return Constant{C.createDouble(C.double(x))};
	 
}

func CreateString(x string) Constant{
   
	return Constant{C.createString(C.CString(x))};
	 
}

func CreateDate(year int, month int, day int) Constant{
   
	return Constant{C.createDate(C.int(year), C.int(month), C.int(day))};
	 
}

func CreateMonth(year int, month int) Constant{
   
	return Constant{C.createMonth(C.int(year), C.int(month))};
	 
}

func CreateNanoTime(hour int, minute int, second int, nanosecond int) Constant{
   
	return Constant{C.createNanoTime(C.int(hour), C.int(minute), C.int(second), C.int(nanosecond))};
	 
}

func CreateTime(hour int, minute int, second int, millisecond int) Constant{
   
	return Constant{C.createTime(C.int(hour), C.int(minute), C.int(second), C.int(millisecond))};
	 
}

func CreateSecond(hour int, minute int, second int) Constant{
   
	return Constant{C.createSecond(C.int(hour), C.int(minute), C.int(second))};
	 
}

func CreateMinute(hour int, minute int) Constant{
   
	return Constant{C.createMinute(C.int(hour), C.int(minute))};
	 
}

func CreateNanoTimestamp(year int, month int, day, hour int, minute int, second int, nanosecond int) Constant{
   
	return Constant{C.createNanoTimestamp(C.int(year), C.int(month), C.int(day),C.int(hour), C.int(minute), C.int(second), C.int(nanosecond))};
	 
}

func CreateTimestamp(year int, month int, day, hour int, minute int, second int, millisecond int) Constant{
   
	return Constant{C.createTimestamp(C.int(year), C.int(month), C.int(day),C.int(hour), C.int(minute), C.int(second), C.int(millisecond))};
	 
}

func CreateDateTime(year int, month int, day, hour int, minute int, second int) Constant{
   
	return Constant{C.createDateTime(C.int(year), C.int(month), C.int(day),C.int(hour), C.int(minute), C.int(second))};
	 
}

func CreateVector(dttype int) Vector{
    
   return Vector{Constant:Constant{ptr:C.createVector(C.int(dttype))}};

}

func CreateTable(colname []string, cols []Vector) Table {
	 l := len(colname); 
//	s := make([]*C.char, 0, l);
//	v := make([]*C.Constant, 0, l);
	s := CreateVector(DT_STRING);
	v := CreateVector(DT_ANY);

	for i := 0; i < l;i++{
//	   s = append(s,C.CString(colname[i]));
       s.Append(CreateString(colname[i]));
	   //v = append(v, cols[i].ptr);
	   v.Append(cols[i].ToConstant());
	 }
	 return Table{Constant:Constant{ptr:C.createTable(s.ptr,v.ptr, C.int(l))}}; 
//	return Table{Constant:Constant{ptr:C.createTable((**C.char)(unsafe.Pointer(&colname[0])),(**C.Constant)(unsafe.Pointer(&v[0])), C.int(l))}};   
//return 0;
	
}

func (c * Constant) ToVector() Vector {
   
//	return Vector{Constant:Constant{ptr:(*C.DBConnection)(unsafe.Pointer(C.toVector(c.ptr)))}};
return Vector{Constant:Constant{ptr:C.toVector(c.ptr)}};

}

func (v *Vector) GetName() string {
	return C.GoString(C.Vector_getName(v.ptr));
}

func (v *Vector) SetName(vname string) {
	C.Vector_setName(v.ptr, C.CString(vname));
}

func (v *Vector) Get(x int) Constant{
	return Constant{ptr:C.Vector_get(v.ptr, C.int(x))};
}

func (v *Vector) Remove(x int) bool {
	return tobool(C.Vector_remove(v.ptr, C.int(x)));
}

func (v *Vector) Append(c Constant) bool {
	return tobool(C.Vector_append(v.ptr, c.ptr));
}

func (v *Vector) GetIntSlice() []int{
	cap := v.Size();
	s := make([]int, 0, cap);
 	for i := 0; i < cap;i++{
		a := v.Get(i);
        s = append(s,a.GetInt());
	 }
	return s;   
} 
func (v *Vector) GetShortSlice() []int16{
	cap := v.Size();
	s := make([]int16, 0, cap);
 	for i := 0; i < cap;i++{
		a := v.Get(i);
        s = append(s,a.GetShort());
	 }
	return s;   
} 
func (v *Vector) GetLongSlice() []int64{
	cap := v.Size();
	s := make([]int64, 0, cap);
 	for i := 0; i < cap;i++{
		a := v.Get(i);
        s = append(s,a.GetLong());
	 }
	return s;   
} 
func (v *Vector) GetBoolSlice() []bool{
	cap := v.Size();
	s := make([]bool, 0, cap);
 	for i := 0; i < cap;i++{
		a := v.Get(i);
        s = append(s,a.GetBool());
	 }
	return s;   
} 
func (v *Vector) GetFloatSlice() []float32{
	cap := v.Size();
	s := make([]float32, 0, cap);
 	for i := 0; i < cap;i++{
		a := v.Get(i);
        s = append(s,a.GetFloat());
	 }
	return s;   
} 
func (v *Vector) GetDoubleSlice() []float64{
	cap := v.Size();
	s := make([]float64, 0, cap);
 	for i := 0; i < cap;i++{
		a := v.Get(i);
        s = append(s,a.GetDouble());
	 }
	return s;   
} 
func (v *Vector) GetStringSlice() []string{
	cap := v.Size();
	s := make([]string, 0, cap);
 	for i := 0; i < cap;i++{
		a := v.Get(i);
        s = append(s,a.GetString());
	 }
	return s;   
} 


func (c * Constant) ToTable() Table {
   
	//	return Vector{Constant:Constant{ptr:(*C.DBConnection)(unsafe.Pointer(C.toVector(c.ptr)))}};
	return Table{Constant:Constant{ptr:C.toTable(c.ptr)}};
}

func (t *Table) SetName(tname string) {
	C.Table_setName(t.ptr, C.CString(tname));
}

func (t *Table) GetName() string {
	return C.GoString(C.Table_getName(t.ptr));
}
func (t *Table) GetColumn(x int) Vector {
	return Vector{Constant:Constant{ptr:C.Table_getColumn(t.ptr, C.int(x))}};
}
func (t *Table) GetColumnbyName(name string) Vector {
	return Vector{Constant:Constant{ptr:C.Table_getColumnbyName(t.ptr, C.CString(name))}};
}
func (t *Table) GetColumnName(x int) string {
	return C.GoString(C.Table_getColumnName(t.ptr, C.int(x)));
}
func (t *Table) Columns() int {
	return int(C.Table_columns(t.ptr));
}
func (t *Table) Rows() int {
	return int(C.Table_rows(t.ptr));
}
func (t *Table) GetColumnType(x int) int {
	return int(C.Table_getColumnType(t.ptr, C.int(x)));
}






func (conn *DBConnection) Init() {
	conn.ptr = C.DBConnection_new()
}

func (conn *DBConnection) Connect(host string, port int, user string, password string) bool {
	return tobool(C.DBConnection_connect(conn.ptr, C.CString(host), C.int(port), C.CString(user), C.CString(password)));
	 //C.CString(startup))	C.int(highAvailiability))
}

func (conn *DBConnection) Run(script string) Constant {
	return Constant{ptr:C.DBConnection_run(conn.ptr, C.CString(script))};
}
func (conn *DBConnection) Upload(name string,c Constant) {
	C.DBConnection_upload(conn.ptr, C.CString(name),c.ptr);
}
func (conn *DBConnection) Close() {
	C.DBConnection_close(conn.ptr);
}
func (conn *DBConnection) RunFunc(script string, args []Constant) Constant {

	l := len(args); 
	//	s := make([]*C.char, 0, l);
	//	v := make([]*C.Constant, 0, l);
	//	s := CreateVector(DT_STRING);
		v := CreateVector(DT_ANY);
	
		for i := 0; i < l;i++{
	//	   s = append(s,C.CString(colname[i]));
//		   s.Append(CreateString(colname[i]));
		   //v = append(v, cols[i].ptr);
		   v.Append(args[i]);
		 }
		 return Constant{ptr:C.DBConnection_runfunc(conn.ptr,C.CString(script),v.ptr)}; 

}


func  (c *Constant) GetBool() bool {
	return tobool(C.Constant_getBool(c.ptr));
}
   

func  (c *Constant) GetInt() int {

 return int(C.Constant_getInt(c.ptr));
}

func (c *Constant) GetForm() int {
	return int(C.Constant_getForm(c.ptr));
}
func (c *Constant) GetType() int {
	return int(C.Constant_getType(c.ptr));
}
func (c *Constant) GetShort() int16 {
	return int16(C.Constant_getShort(c.ptr));
}
func (c *Constant) GetLong() int64 {
	return int64(C.Constant_getLong(c.ptr));
}
func (c *Constant) GetFloat() float32 {
	return float32(C.Constant_getFloat(c.ptr));
}
func (c *Constant) GetDouble() float64 {
	return float64(C.Constant_getDouble(c.ptr));
}
func (c *Constant) GetString() string {
	return C.GoString(C.Constant_getString(c.ptr));
}



func (c *Constant) Size() int {
	return int(C.Constant_size(c.ptr));
}
func (c *Constant) IsScalar() bool{
	return tobool(C.Constant_isScalar(c.ptr));
}
func (c *Constant) IsArray() bool{
	return tobool(C.Constant_isArray(c.ptr));
}
func (c *Constant) IsPair() bool{
	return tobool(C.Constant_isPair(c.ptr));
}
func (c *Constant) IsMatrix() bool{
	return tobool(C.Constant_isMatrix(c.ptr));
}
func (c *Constant) IsVector() bool{
	return tobool(C.Constant_isVector(c.ptr));
}
func (c *Constant) IsTable() bool{
	return tobool(C.Constant_isTable(c.ptr));
}
func (c *Constant) IsSet() bool{
	return tobool(C.Constant_isSet(c.ptr));
}
func (c *Constant) IsDictionary() bool{
	return tobool(C.Constant_isDictionary(c.ptr));
}

func (c *Constant) ToConstant() Constant{
   return Constant{C.toConstant(c.ptr)};
}
/*
func main() {
	conn := new(DBConnection);
	conn.Init();
	fmt.Println(conn.Connect("localhost",1621,"admin","123456"));

//	var p Constant;
	conn.Run("t1=table(1 2 3 as a, `x`y`z as b, 10.8 7.6 3.5 as c);");
	p1 := conn.Run("t1");
	conn.Run("x = 3 6 1 5 9");
//	p = conn.Run("x");
//	var y Vector = p.ToVector();
//	 z := y.Get(1);
    var t Table = p1.ToTable();
	fmt.Println(t.GetString());
	fmt.Println(t.Columns(),t.GetColumnType(1));
	y := t.GetColumnbyName("b");
	fmt.Println(y.GetString());
	s := y.GetStringSlice();
	fmt.Println(s);
	
	v1 := CreateVector(DT_INT);
	v2 := CreateVector(DT_INT)
	cols := [] Vector {v1,v2};
	v1.Append(CreateInt(1));
	v2.Append(CreateInt(1));
	colnames := [] string {"v1","v2"};
	ta := CreateTable(colnames, cols);
    fmt.Println(ta.GetString());
	

  //  a := []int {1,2,3};

	tb := ta.ToConstant();
	args := [] Constant{tb};
	conn.Upload("tglobal",tb);
	conn.RunFunc("tableInsert{tglobal}", args); 
	
	x := conn.Run("1+1");
	fmt.Println(x.GetForm(),x.GetType());

//	 conn.Close();
//	 p = conn.Run("1+1");
}
*/