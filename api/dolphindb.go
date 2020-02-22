package ddb

/*
typedef struct DBConnection DBConnection;
typedef struct PollingClient PollingClient;
typedef struct Constant Constant;
typedef struct MessageQueue MessageQueue;

int Constant_isNull(Constant* w);
char* def_action_name();

PollingClient* PollingClient_new(int port);
MessageQueue*  PollingClient_subscribe(PollingClient* client, char* host, int port, char*  tableName,char*  actionName ,   long long offset);
void PollingClient_unsubscribe(PollingClient* client, char* host, int port, char* tableName, char* actionName);

int MessageQueue_poll(MessageQueue* w,  Constant* msg,  int s);

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

Constant* Constant_get(Constant* w, int x);

int Constant_isScalar(Constant* w);
int Constant_isArray(Constant* w);
int Constant_isPair(Constant* w);
int Constant_isMatrix(Constant* w);
int Constant_isVector(Constant* w);
int Constant_isTable(Constant* w);
int Constant_isSet(Constant* w);
int Constant_isDictionary(Constant* w);

int Constant_size(Constant* w);


int Constant_setBoolArray(Constant* w, int start, int len, char* buf);
int Constant_setIntArray(Constant* w, int start, int len, int* buf);
int Constant_setLongArray(Constant* w, int start, int len, long long* buf);
int Constant_setShortArray(Constant* w, int start, int len, short* buf);
int Constant_setFloatArray(Constant* w, int start, int len, float* buf);
int Constant_setDoubleArray(Constant* w, int start, int len, double* buf);
int Constant_setStringArray(Constant* w, int start, int len, char* buf);

void Constant_setIntByIndex(Constant* w, int index, int val);
void Constant_setBoolByIndex(Constant* w, int index, short val);
void Constant_setShortByIndex(Constant* w, int index, short val);
void Constant_setLongByIndex(Constant* w, int index, long long val);
void Constant_setFloatByIndex(Constant* w, int index, float val);
void Constant_setDoubleByIndex(Constant* w, int index, double val);
void Constant_setStringByIndex(Constant* w, int index, char* val);
void Constant_setNullByIndex(Constant* w, int index);

void Constant_setInt(Constant* w, int val);
void Constant_setBool(Constant* w, short val);
void Constant_setShort(Constant* w, short val);
void Constant_setLong(Constant* w, long long val);
void Constant_setFloat(Constant* w, float val);
void Constant_setDouble(Constant* w, double val);
void Constant_setString(Constant* w, char* val);
void Constant_setNull(Constant* w);

void delConstant(Constant* w);

int Constant_setByIndex(Constant*w, int index, Constant* x);

Constant*  toConstant(Constant* w);

Constant*  toSet(Constant* w);
Constant*  toMatrix(Constant* w);
Constant*  toDictionary(Constant* w);

Constant*  toVector(Constant* w);
void Vector_setName(Constant* w, char* vname);
char* Vector_getName(Constant* w);

int Vector_remove(Constant* v,int x);
int Vector_append(Constant* v,Constant* w);
int Vector_appendInt(Constant* v, int* x, int len);
int Vector_appendShort(Constant* v, short * x, int len);
int Vector_appendLong(Constant* v, long long* x, int len);
int Vector_appendFloat(Constant* v, float* x, int len);
int Vector_appendDouble(Constant* v, double* x, int len);
int Vector_appendString(Constant* v, char* x, int len);
int Vector_appendBool(Constant* v, char* x, int len);

Constant* Vector_getColumnLabel(Constant* w);
int Vector_isView(Constant* w);
void Vector_initialize(Constant* w);
int Vector_getCapacity(Constant* w);
int Vector_reserve(Constant* w, int x);
int Vector_getUnitLength(Constant* w);
void Vector_clear(Constant* w);
int Vector_removebyIndex(Constant* w, Constant* index);
Constant* Vector_getInstance(Constant* w, int size);
Constant* Vector_getSubVector(Constant* w, int start,int length);
void Vector_fill(Constant* w, int start,int l, Constant* val);
void Vector_next(Constant* w, int steps);
void Vector_prev(Constant* w, int steps);
void Vector_reverse(Constant* w);
void Vector_reverseSegment(Constant* w, int start, int l);
void Vector_replace(Constant* w, Constant* oldval, Constant* newval);
int Vector_validIndex(Constant* w, int index);
void Vector_addIndex(Constant* w, int start, int l, int offset);
void Vector_neg(Constant* w);

Constant*  toTable(Constant* w);
void Table_setName(Constant* w, char* tname);
char* Table_getName(Constant* w);

char* Table_getColumnName(Constant* w,int x);
int Table_columns(Constant* w);
int Table_rows(Constant* w);
int Table_getColumnType(Constant* w,int x);
Constant* Table_getColumnbyName(Constant* w,char* s);
Constant* Table_getColumn(Constant* w,int x);

char* Table_getScript(Constant* w);
char* Table_getColumnQualifier(Constant* w, int index);
void Table_setColumnName(Constant* w, int index, char* name);
int Table_getColumnIndex(Constant* w, char* name);
int Table_contain(Constant* w, char* name);
Constant* Table_getValue(Constant* w);
Constant* Table_getInstance(Constant* w, int size);
int Table_sizeable(Constant* w);
char* Table_getStringbyIndex(Constant* w, int index);
Constant* Table_getWindow(Constant* w, int colStart, int colLength, int rowStart, int rowLength);
Constant* Table_getMember(Constant* w,Constant* key);
Constant* Table_values(Constant* w);
Constant* Table_keys(Constant* w);
int Table_getTableType(Constant* w);
void Table_drop(Constant* w, Constant* v);

void Set_clear(Constant* w);
int Set_remove(Constant* w, Constant* val);
int Set_append(Constant* w, Constant* val);
int Set_inverse(Constant* w, Constant* val);
void Set_contain(Constant* w, Constant* target, Constant* result);
int Set_isSuperSet(Constant* w, Constant* target);
char*  Set_getScript(Constant* w);
Constant* Set_interaction(Constant* w, Constant* target);
Constant* Set_getSubVector(Constant* w, int start, int length);

void Matrix_setRowLabel(Constant* w, Constant* label);
void Matrix_setColumnLabel(Constant* w, Constant* label);
int Matrix_reshape(Constant* w, int cols, int rows);
Constant* Matrix_getColumn(Constant* w,int x);
char* Matrix_getStringbyIndex(Constant* w, int x);
char* Matrix_getCellString(Constant* w, int x, int y);
Constant* Matrix_getInstance(Constant* w, int size);
int Matrix_setColumn(Constant* w,int index, Constant* col);

int Dictionary_count(Constant* w);
void Dictionary_clear(Constant* w);
Constant* Dictionary_getMember(Constant* w, Constant* key);
int Dictionary_getKeyType(Constant* w);
Constant* Dictionary_keys(Constant* w);
Constant* Dictionary_values(Constant* w);
char* Dictionary_getScript(Constant* w);
int Dictionary_remove(Constant* w, Constant* key);
int Dictionary_set(Constant* w, Constant* key, Constant* value);
void Dictionary_contain(Constant* w, Constant* target, Constant* result);

int Constant_isLargeConstant(Constant* w);

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

Constant* createVector(int type, int size);
Constant* createTable(Constant* colname,Constant* coltypes,int size, int capacity, int len);
Constant* createTableByVector(Constant* colname,Constant* cols,int len);

Constant* createConstant(int type);
void Constant_setBinary(Constant* w, char* val);
void Constant_setBinaryByIndex(Constant* w, int index, char* val);
int Constant_setBinaryArray(Constant* w, int start, int len, char* buf);
Constant* parseConstant(int type, char* word);

int Constant_getHash(Constant* w, int buckets);
int Constant_getHashArray(Constant* w, int start, int len, int buckets, int* buf);
long long getEpochTime();
#cgo LDFLAGS: -L./ -lwrapper -Wl,-rpath,./api/
*/
import "C"
import "unsafe"
import "fmt"

const (
	hostname = "localhost"
	port     = 8848
	user     = "admin"
	pass     = "123456"
)

const (
	DT_VOID = iota
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
	DT_DATEHOUR
	DT_DATEMINUTE
	DT_IP
	DT_INT128
	DT_OBJECT
)
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
)

func tobool(x C.int) bool {
	if int(x) != 0 {
		return true
	}
	return false
}

func (c *Constant) IsNull() bool {
	return tobool(C.Constant_isNull(c.ptr))
}
func Def_action_name() string {
	return C.GoString(C.def_action_name())
}

type MessageQueue struct {
	ptr *C.MessageQueue
}

func (client *PollingClient) Subscribe(host string, port int, tableName string, actionName string, offset int64) MessageQueue {
	return MessageQueue{ptr: C.PollingClient_subscribe(client.ptr, C.CString(host), C.int(port), C.CString(tableName), C.CString(actionName), C.longlong(offset))}
}

func (client *PollingClient) UnSubscribe(host string, port int, tableName string, actionName string) {
	C.PollingClient_unsubscribe(client.ptr, C.CString(host), C.int(port), C.CString(tableName), C.CString(actionName))
}

//void PollingClient_unsubscribe(PollingClient* client, char* host, int port, char* tableName, char* actionName);

func (queue *MessageQueue) Poll(msg Constant, s int) bool {
	return tobool(C.MessageQueue_poll(queue.ptr, msg.ptr, C.int(s)))
}

type PollingClient struct {
	ptr *C.PollingClient
}

func (client *PollingClient) New(listenport int) {
	client.ptr = C.PollingClient_new(C.int(listenport))
}

type DBConnection struct {
	ptr *C.DBConnection
}

type Constant struct {
	ptr *C.Constant
}

type Vector struct {
	Constant
}

type Table struct {
	Constant
}

type Set struct {
	Constant
}

type Matrix struct {
	Constant
}

type Dictionary struct {
	Constant
}

func CreateInt(x int) Constant {

	return Constant{C.createInt(C.int(x))}

}

func CreateBool(x bool) Constant {
	var y int
	if x == true {
		y = 1
	} else {
		y = 0
	}
	return Constant{C.createBool(C.int(y))}

}

func CreateShort(x int16) Constant {

	return Constant{C.createShort(C.short(x))}

}

func CreateLong(x int64) Constant {

	return Constant{C.createLong(C.longlong(x))}

}

func CreateFloat(x float32) Constant {

	return Constant{C.createFloat(C.float(x))}

}
func CreateDouble(x float64) Constant {

	return Constant{C.createDouble(C.double(x))}

}

func CreateString(x string) Constant {

	return Constant{C.createString(C.CString(x))}

}

func CreateDate(year int, month int, day int) Constant {

	return Constant{C.createDate(C.int(year), C.int(month), C.int(day))}

}

func CreateMonth(year int, month int) Constant {

	return Constant{C.createMonth(C.int(year), C.int(month))}

}

func CreateNanoTime(hour int, minute int, second int, nanosecond int) Constant {

	return Constant{C.createNanoTime(C.int(hour), C.int(minute), C.int(second), C.int(nanosecond))}

}

func CreateTime(hour int, minute int, second int, millisecond int) Constant {

	return Constant{C.createTime(C.int(hour), C.int(minute), C.int(second), C.int(millisecond))}

}

func CreateSecond(hour int, minute int, second int) Constant {

	return Constant{C.createSecond(C.int(hour), C.int(minute), C.int(second))}

}

func CreateMinute(hour int, minute int) Constant {

	return Constant{C.createMinute(C.int(hour), C.int(minute))}

}

func CreateNanoTimestamp(year int, month int, day, hour int, minute int, second int, nanosecond int) Constant {

	return Constant{C.createNanoTimestamp(C.int(year), C.int(month), C.int(day), C.int(hour), C.int(minute), C.int(second), C.int(nanosecond))}

}

func CreateTimestamp(year int, month int, day, hour int, minute int, second int, millisecond int) Constant {

	return Constant{C.createTimestamp(C.int(year), C.int(month), C.int(day), C.int(hour), C.int(minute), C.int(second), C.int(millisecond))}

}

func CreateDateTime(year int, month int, day, hour int, minute int, second int) Constant {

	return Constant{C.createDateTime(C.int(year), C.int(month), C.int(day), C.int(hour), C.int(minute), C.int(second))}

}

func CreateVector(dttype int, size int) Vector {

	return Vector{Constant: Constant{ptr: C.createVector(C.int(dttype), C.int(size))}}

}

func CreateTable(colname []string, coltype []int, size int, capacity int) Table {
	l := len(colname)
	//	s := make([]*C.char, 0, l);
	//	v := make([]*C.Constant, 0, l);
	s := CreateVector(DT_STRING, 0)
	v := CreateVector(DT_INT, 0)

	for i := 0; i < l; i++ {
		//	   s = append(s,C.CString(colname[i]));
		s.Append(CreateString(colname[i]))
		v.Append(CreateInt(coltype[i]))
		//v = append(v, cols[i].ptr);

	}
	return Table{Constant: Constant{ptr: C.createTable(s.ptr, v.ptr, C.int(size), C.int(capacity), C.int(l))}}
	//	return Table{Constant:Constant{ptr:C.createTable(s.ptr, (*C.int)(unsafe.Pointer(&coltype[0]))  , C.int(size), C.int(capacity), C.int(l))}};
	//	return Table{Constant:Constant{ptr:C.createTable((**C.char)(unsafe.Pointer(&colname[0])),(**C.Constant)(unsafe.Pointer(&v[0])), C.int(l))}};
	//return 0;

}

func CreateTableByVector(colname []string, cols []Vector) Table {
	l := len(colname)
	//	s := make([]*C.char, 0, l);
	//	v := make([]*C.Constant, 0, l);
	s := CreateVector(DT_STRING, 0)
	v := CreateVector(DT_ANY, 0)

	for i := 0; i < l; i++ {
		//	   s = append(s,C.CString(colname[i]));
		s.Append(CreateString(colname[i]))
		//v = append(v, cols[i].ptr);
		v.Append(cols[i].ToConstant())
	}
	return Table{Constant: Constant{ptr: C.createTableByVector(s.ptr, v.ptr, C.int(l))}}
	//	return Table{Constant:Constant{ptr:C.createTable((**C.char)(unsafe.Pointer(&colname[0])),(**C.Constant)(unsafe.Pointer(&v[0])), C.int(l))}};
	//return 0;

}

func (c *Constant) ToVector() Vector {

	//	return Vector{Constant:Constant{ptr:(*C.DBConnection)(unsafe.Pointer(C.toVector(c.ptr)))}};
	return Vector{Constant: Constant{ptr: C.toVector(c.ptr)}}

}

func (v *Vector) GetName() string {
	return C.GoString(C.Vector_getName(v.ptr))
}

func (v *Vector) SetName(vname string) {
	C.Vector_setName(v.ptr, C.CString(vname))
}

func (v *Vector) Remove(x int) bool {
	return tobool(C.Vector_remove(v.ptr, C.int(x)))
}

func (v *Vector) Append(c Constant) bool {
	return tobool(C.Vector_append(v.ptr, c.ptr))
}

func (v *Vector) AppendBool(x []bool, len int) bool {

	return tobool(C.Vector_appendBool(v.ptr, (*C.char)(unsafe.Pointer(&x[0])), C.int(len)))
}

func (v *Vector) AppendInt(x []int32, len int) bool {

	return tobool(C.Vector_appendInt(v.ptr, (*C.int)(unsafe.Pointer(&x[0])), C.int(len)))
}

func (v *Vector) AppendShort(x []int16, len int) bool {

	return tobool(C.Vector_appendShort(v.ptr, (*C.short)(unsafe.Pointer(&x[0])), C.int(len)))
}

func (v *Vector) AppendLong(x []int64, len int) bool {

	return tobool(C.Vector_appendLong(v.ptr, (*C.longlong)(unsafe.Pointer(&x[0])), C.int(len)))
}

func (v *Vector) AppendFloat(x []float32, len int) bool {

	return tobool(C.Vector_appendFloat(v.ptr, (*C.float)(unsafe.Pointer(&x[0])), C.int(len)))
}

func (v *Vector) AppendDouble(x []float64, len int) bool {

	return tobool(C.Vector_appendDouble(v.ptr, (*C.double)(unsafe.Pointer(&x[0])), C.int(len)))
}

func (v *Vector) AppendString(x []string, len int) bool {

	for i := 0; i < len; i++ {
		if tobool(C.Vector_appendString(v.ptr, C.CString(x[i]), C.int(1))) != true {
			return false
		}
	}
	return true
	//return tobool(C.Vector_appendString(v.ptr, C.CString(x[0]),C.int(l)));
}

func (v *Vector) GetIntSlice() []int {
	cap := v.Size()
	s := make([]int, 0, cap)
	for i := 0; i < cap; i++ {
		a := v.Get(i)
		s = append(s, a.GetInt())
	}
	return s
}
func (v *Vector) GetShortSlice() []int16 {
	cap := v.Size()
	s := make([]int16, 0, cap)
	for i := 0; i < cap; i++ {
		a := v.Get(i)
		s = append(s, a.GetShort())
	}
	return s
}
func (v *Vector) GetLongSlice() []int64 {
	cap := v.Size()
	s := make([]int64, 0, cap)
	for i := 0; i < cap; i++ {
		a := v.Get(i)
		s = append(s, a.GetLong())
	}
	return s
}
func (v *Vector) GetBoolSlice() []bool {
	cap := v.Size()
	s := make([]bool, 0, cap)
	for i := 0; i < cap; i++ {
		a := v.Get(i)
		s = append(s, a.GetBool())
	}
	return s
}
func (v *Vector) GetFloatSlice() []float32 {
	cap := v.Size()
	s := make([]float32, 0, cap)
	for i := 0; i < cap; i++ {
		a := v.Get(i)
		s = append(s, a.GetFloat())
	}
	return s
}
func (v *Vector) GetDoubleSlice() []float64 {
	cap := v.Size()
	s := make([]float64, 0, cap)
	for i := 0; i < cap; i++ {
		a := v.Get(i)
		s = append(s, a.GetDouble())
	}
	return s
}
func (v *Vector) GetStringSlice() []string {
	cap := v.Size()
	s := make([]string, 0, cap)
	for i := 0; i < cap; i++ {
		a := v.Get(i)
		s = append(s, a.GetString())
	}
	return s
}

func (v *Vector) GetColumnLabel() Constant {
	return Constant{C.Vector_getColumnLabel(v.ptr)}
}

func (v *Vector) IsView() bool {
	return tobool(C.Vector_isView(v.ptr))
}

func (v *Vector) Initialize() {
	C.Vector_initialize(v.ptr)
}

func (v *Vector) GetCapacity() int {
	return int(C.Vector_getCapacity(v.ptr))
}

func (v *Vector) Reserve(capacity int) int {
	return int(C.Vector_reserve(v.ptr, C.int(capacity)))
}

func (v *Vector) GetUnitLength() int {
	return int(C.Vector_getUnitLength(v.ptr))
}

func (v *Vector) Clear() {
	C.Vector_clear(v.ptr)
}

func (v *Vector) RemovebyIndex(index Constant) bool {
	return tobool(C.Vector_removebyIndex(v.ptr, index.ptr))
}

func (v *Vector) GetInstance(size int) Constant {
	return Constant{C.Vector_getInstance(v.ptr, C.int(size))}
}

func (v *Vector) GetSubVector(start int, length int) Constant {
	return Constant{C.Vector_getSubVector(v.ptr, C.int(start), C.int(length))}
}

func (v *Vector) Fill(start int, length int, val Constant) {
	C.Vector_fill(v.ptr, C.int(start), C.int(length), val.ptr)
}

func (v *Vector) Next(steps int) {
	C.Vector_next(v.ptr, C.int(steps))
}

func (v *Vector) Prev(steps int) {
	C.Vector_prev(v.ptr, C.int(steps))
}

func (v *Vector) Reverse() {
	C.Vector_reverse(v.ptr)
}

func (v *Vector) ReverseSegMent(start int, length int) {
	C.Vector_reverseSegment(v.ptr, C.int(start), C.int(length))
}

func (v *Vector) Replace(oldval Constant, newval Constant) {
	C.Vector_replace(v.ptr, oldval.ptr, newval.ptr)
}

func (v *Vector) ValidIndex(index int) bool {
	return tobool(C.Vector_validIndex(v.ptr, C.int(index)))
}

func (v *Vector) AddIndex(start int, length int, offset int) {
	C.Vector_addIndex(v.ptr, C.int(start), C.int(length), C.int(offset))
}

func (v *Vector) Neg() {
	C.Vector_neg(v.ptr)
}

/*
 char* Table_getScript(Constant* w);
 char* Table_getColumnQualifier(Constant* w, int index);
 void Table_setColumnName(Constant* w, int index, char* name);
 int Table_getColumnIndex(Constant* w, char* name);
 int Table_contain(Constant* w, char* name);
 Constant* Table_getValue(Constant* w);
 Constant* Table_getInstance(Constant* w, int size);
 int Table_sizeable(Constant* w);
 char* Table_getStringbyIndex(Constant* w, int index);
 Constant* Table_getWindow(Constant* w, int colStart, int colLength, int rowStart, int rowLength);
 Constant* Table_getMember(Constant* w,Constant* key);
 Constant* Table_values(Constant* w);
 Constant* Table_keys(Constant* w);
 int Table_getTableType(Constant* w);
 void Table_drop(Constant* w, Constant* v);
*/
func (t *Table) GetScript() string {
	return C.GoString(C.Table_getScript(t.ptr))
}

func (t *Table) GetColumnQualifier(index int) string {
	return C.GoString(C.Table_getColumnQualifier(t.ptr, C.int(index)))
}

func (t *Table) SetColumnName(index int, name string) {
	C.Table_setColumnName(t.ptr, C.int(index), C.CString(name))
}

func (t *Table) GetColumnIndex(name string) int {
	return int(C.Table_getColumnIndex(t.ptr, C.CString(name)))
}

func (t *Table) Contain(name string) bool {
	return tobool(C.Table_contain(t.ptr, C.CString(name)))
}

func (t *Table) GetValue() Constant {

	return Constant{C.Table_getValue(t.ptr)}
}

func (t *Table) GetInstance(size int) Constant {

	return Constant{C.Table_getInstance(t.ptr, C.int(size))}
}

func (t *Table) Sizeable(name string) bool {
	return tobool(C.Table_sizeable(t.ptr))
}

func (t *Table) GetStringByIndex(index int) string {
	return C.GoString(C.Table_getStringbyIndex(t.ptr, C.int(index)))
}

func (t *Table) GetWindow(colstart int, collen int, rowstart int, rowlen int) Constant {

	return Constant{C.Table_getWindow(t.ptr, C.int(colstart), C.int(collen), C.int(rowstart), C.int(rowlen))}
}

func (t *Table) GetMember(key Constant) Constant {

	return Constant{C.Table_getMember(t.ptr, key.ptr)}
}

func (t *Table) Values() Constant {

	return Constant{C.Table_values(t.ptr)}
}

func (t *Table) Keys() Constant {

	return Constant{C.Table_keys(t.ptr)}
}

func (t *Table) GetTableType() int {
	return int(C.Table_getTableType(t.ptr))
}

func (t *Table) Drop(cols []int) {
	l := len(cols)
	vec := CreateVector(DT_INT, 0)
	for i := 0; i < l; i++ {

		vec.Append(CreateInt(cols[i]))

	}
	C.Table_drop(t.ptr, vec.ptr)
}

func (c *Constant) ToSet() Set {
	return Set{Constant: Constant{ptr: C.toSet(c.ptr)}}
}

func (c *Constant) ToMatrix() Matrix {
	return Matrix{Constant: Constant{ptr: C.toMatrix(c.ptr)}}
}

func (c *Constant) ToDictionary() Dictionary {
	return Dictionary{Constant: Constant{ptr: C.toDictionary(c.ptr)}}
}

func (c *Constant) ToTable() Table {

	//	return Vector{Constant:Constant{ptr:(*C.DBConnection)(unsafe.Pointer(C.toVector(c.ptr)))}};
	return Table{Constant: Constant{ptr: C.toTable(c.ptr)}}
}

func (t *Table) SetName(tname string) {
	C.Table_setName(t.ptr, C.CString(tname))
}

func (t *Table) GetName() string {
	return C.GoString(C.Table_getName(t.ptr))
}
func (t *Table) GetColumn(x int) Vector {
	return Vector{Constant: Constant{ptr: C.Table_getColumn(t.ptr, C.int(x))}}
}
func (t *Table) GetColumnByName(name string) Vector {
	return Vector{Constant: Constant{ptr: C.Table_getColumnbyName(t.ptr, C.CString(name))}}
}
func (t *Table) GetColumnName(x int) string {
	return C.GoString(C.Table_getColumnName(t.ptr, C.int(x)))
}
func (t *Table) Columns() int {
	return int(C.Table_columns(t.ptr))
}
func (t *Table) Rows() int {
	return int(C.Table_rows(t.ptr))
}
func (t *Table) GetColumnType(x int) int {
	return int(C.Table_getColumnType(t.ptr, C.int(x)))
}

/*
void Matrix_setRowLabel(Constant* w, Constant* label);
void Matrix_setColumnLabel(Constant* w, Constant* label);
int Matrix_reshape(Constant* w, int cols, int rows);
Constant* Matrix_getColumn(Constant* w,int x);
char* Matrix_getStringbyIndex(Constant* w, int x);
char* Matrix_getCellString(Constant* w, int x, int y);
Constant* Matrix_getInstance(Constant* w, int size);
int Matrix_setColumn(Constant* w,int index, Constant* col);
*/
func (m *Matrix) SetRowLabel(label Constant) {

	C.Matrix_setRowLabel(m.ptr, label.ptr)
}
func (m *Matrix) SetColumnLabel(label Constant) {

	C.Matrix_setColumnLabel(m.ptr, label.ptr)
}

func (m *Matrix) Reshape(cols int, rows int) bool {
	return tobool(C.Matrix_reshape(m.ptr, C.int(cols), C.int(rows)))
}

func (m *Matrix) GetColumn(x int) Vector {
	return Vector{Constant: Constant{ptr: C.Matrix_getColumn(m.ptr, C.int(x))}}
}

func (m *Matrix) GetStringbyIndex(x int) string {
	return C.GoString(C.Matrix_getStringbyIndex(m.ptr, C.int(x)))
}

func (m *Matrix) GetCellString(col int, row int) string {
	return C.GoString(C.Matrix_getCellString(m.ptr, C.int(col), C.int(row)))
}

func (m *Matrix) GetInstance(size int) Constant {
	return Constant{C.Matrix_getInstance(m.ptr, C.int(size))}
}

func (m *Matrix) SetColumn(index int, col Constant) bool {
	return tobool(C.Matrix_setColumn(m.ptr, C.int(index), col.ptr))

}

func (d *Dictionary) Count() int {

	return int(C.Dictionary_count(d.ptr))
}

func (d *Dictionary) Clear() {

	C.Dictionary_clear(d.ptr)
}

func (d *Dictionary) GetMember(key Constant) Constant {
	return Constant{C.Dictionary_getMember(d.ptr, key.ptr)}
}

func (d *Dictionary) GetKeyType() int {

	return int(C.Dictionary_getKeyType(d.ptr))
}

func (d *Dictionary) Keys() Constant {
	return Constant{C.Dictionary_keys(d.ptr)}
}

func (d *Dictionary) Values() Constant {
	return Constant{C.Dictionary_values(d.ptr)}
}

func (d *Dictionary) GetScript() string {
	return C.GoString(C.Dictionary_getScript(d.ptr))
}

func (d *Dictionary) Remove(key Constant) bool {
	return tobool(C.Dictionary_remove(d.ptr, key.ptr))
}

func (d *Dictionary) Set(key Constant, value Constant) bool {
	return tobool(C.Dictionary_set(d.ptr, key.ptr, value.ptr))
}

func (d *Dictionary) Contain(target Constant, result Constant) {
	C.Dictionary_contain(d.ptr, target.ptr, result.ptr)
}

func (s *Set) Clear() {
	C.Set_clear(s.ptr)
}

func (s *Set) Remove(c Constant) bool {
	return tobool(C.Set_remove(s.ptr, c.ptr))
}
func (s *Set) Append(c Constant) bool {
	return tobool(C.Set_append(s.ptr, c.ptr))
}
func (s *Set) Inverse(c Constant) bool {
	return tobool(C.Set_inverse(s.ptr, c.ptr))
}

func (s *Set) Contain(c Constant, r Constant) {
	C.Set_contain(s.ptr, c.ptr, r.ptr)
}

func (s *Set) IsSuperSet(c Constant) bool {
	return tobool(C.Set_isSuperSet(s.ptr, c.ptr))
}

func (s *Set) GetScript() string {
	return C.GoString(C.Set_getScript(s.ptr))
}

func (s *Set) Interaction(c Constant) Constant {
	return Constant{C.Set_interaction(s.ptr, c.ptr)}
}

func (s *Set) GetSubVector(start int, l int) Constant {
	return Constant{C.Set_getSubVector(s.ptr, C.int(start), C.int(l))}
}

func (conn *DBConnection) Init() {
	conn.ptr = C.DBConnection_new()
}

func (conn *DBConnection) Connect(host string, port int, user string, password string) bool {
	return tobool(C.DBConnection_connect(conn.ptr, C.CString(host), C.int(port), C.CString(user), C.CString(password)))
	//C.CString(startup))	C.int(highAvailiability))
}

func (conn *DBConnection) Run(script string) Constant {
	return Constant{ptr: C.DBConnection_run(conn.ptr, C.CString(script))}
}
func (conn *DBConnection) Upload(name string, c Constant) {
	C.DBConnection_upload(conn.ptr, C.CString(name), c.ptr)
}
func (conn *DBConnection) Close() {
	C.DBConnection_close(conn.ptr)
}
func (conn *DBConnection) RunFunc(script string, args []Constant) Constant {

	l := len(args)
	//	s := make([]*C.char, 0, l);
	//	v := make([]*C.Constant, 0, l);
	//	s := CreateVector(DT_STRING);
	v := CreateVector(DT_ANY, 0)

	for i := 0; i < l; i++ {
		//	   s = append(s,C.CString(colname[i]));
		//		   s.Append(CreateString(colname[i]));
		//v = append(v, cols[i].ptr);
		v.Append(args[i])
	}
	return Constant{ptr: C.DBConnection_runfunc(conn.ptr, C.CString(script), v.ptr)}

}

func (c *Constant) IsLargeConstant() bool {

	return tobool(C.Constant_isLargeConstant(c.ptr))
}

func (c *Constant) Get(x int) Constant {
	return Constant{ptr: C.Constant_get(c.ptr, C.int(x))}
}

func (c *Constant) GetBool() bool {
	return tobool(C.Constant_getBool(c.ptr))
}

func (c *Constant) GetInt() int {

	return int(C.Constant_getInt(c.ptr))
}

func (c *Constant) GetForm() int {
	return int(C.Constant_getForm(c.ptr))
}
func (c *Constant) GetType() int {
	return int(C.Constant_getType(c.ptr))
}
func (c *Constant) GetShort() int16 {
	return int16(C.Constant_getShort(c.ptr))
}
func (c *Constant) GetLong() int64 {
	return int64(C.Constant_getLong(c.ptr))
}
func (c *Constant) GetFloat() float32 {
	return float32(C.Constant_getFloat(c.ptr))
}
func (c *Constant) GetDouble() float64 {
	return float64(C.Constant_getDouble(c.ptr))
}
func (c *Constant) GetString() string {
	return C.GoString(C.Constant_getString(c.ptr))
}

func (c *Constant) Size() int {
	return int(C.Constant_size(c.ptr))
}
func (c *Constant) IsScalar() bool {
	return tobool(C.Constant_isScalar(c.ptr))
}
func (c *Constant) IsArray() bool {
	return tobool(C.Constant_isArray(c.ptr))
}
func (c *Constant) IsPair() bool {
	return tobool(C.Constant_isPair(c.ptr))
}
func (c *Constant) IsMatrix() bool {
	return tobool(C.Constant_isMatrix(c.ptr))
}
func (c *Constant) IsVector() bool {
	return tobool(C.Constant_isVector(c.ptr))
}
func (c *Constant) IsTable() bool {
	return tobool(C.Constant_isTable(c.ptr))
}
func (c *Constant) IsSet() bool {
	return tobool(C.Constant_isSet(c.ptr))
}
func (c *Constant) IsDictionary() bool {
	return tobool(C.Constant_isDictionary(c.ptr))
}

func (c *Constant) ToConstant() Constant {

	return Constant{C.toConstant(c.ptr)}

}

func (c *Constant) SetBoolArray(start int, len int, x []bool) bool {

	return tobool(C.Constant_setBoolArray(c.ptr, C.int(start), C.int(len), (*C.char)(unsafe.Pointer(&x[0]))))

}

func (c *Constant) SetIntArray(start int, len int, x []int32) bool {

	return tobool(C.Constant_setIntArray(c.ptr, C.int(start), C.int(len), (*C.int)(unsafe.Pointer(&x[0]))))

}
func (c *Constant) SetLongArray(start int, len int, x []int64) bool {

	return tobool(C.Constant_setLongArray(c.ptr, C.int(start), C.int(len), (*C.longlong)(unsafe.Pointer(&x[0]))))
}
func (c *Constant) SetShortArray(start int, len int, x []int16) bool {

	return tobool(C.Constant_setShortArray(c.ptr, C.int(start), C.int(len), (*C.short)(unsafe.Pointer(&x[0]))))

}
func (c *Constant) SetFloatArray(start int, len int, x []float32) bool {
	return tobool(C.Constant_setFloatArray(c.ptr, C.int(start), C.int(len), (*C.float)(unsafe.Pointer(&x[0]))))
}
func (c *Constant) SetDoubleArray(start int, len int, x []float64) bool {
	return tobool(C.Constant_setDoubleArray(c.ptr, C.int(start), C.int(len), (*C.double)(unsafe.Pointer(&x[0]))))
}
func (c *Constant) SetStringArray(start int, len int, x []string) bool {
	for i := 0; i < len; i++ {
		if tobool(C.Constant_setStringArray(c.ptr, C.int(start+i), C.int(1), C.CString(x[i]))) != true {
			return false
		}

	}
	return true
}

func booltoshort(x bool) int16 {
	if x {
		return 1
	}
	return 0

}

func (c *Constant) SetIntByIndex(index int, x int32) {

	C.Constant_setIntByIndex(c.ptr, C.int(index), C.int(x))

}

func (c *Constant) SetBoolByIndex(index int, x bool) {

	C.Constant_setBoolByIndex(c.ptr, C.int(index), C.short(booltoshort(x)))

}

func (c *Constant) SetShortByIndex(index int, x int16) {

	C.Constant_setShortByIndex(c.ptr, C.int(index), C.short(x))

}

func (c *Constant) SetLongByIndex(index int, x int64) {

	C.Constant_setLongByIndex(c.ptr, C.int(index), C.longlong(x))

}

func (c *Constant) SetFloatByIndex(index int, x float32) {

	C.Constant_setFloatByIndex(c.ptr, C.int(index), C.float(x))

}

func (c *Constant) SetDoubleByIndex(index int, x float64) {

	C.Constant_setDoubleByIndex(c.ptr, C.int(index), C.double(x))

}

func (c *Constant) SetStringByIndex(index int, x string) {

	C.Constant_setStringByIndex(c.ptr, C.int(index), C.CString(x))

}

func (c *Constant) SetNullByIndex(index int) {

	C.Constant_setNullByIndex(c.ptr, C.int(index))

}

func (c *Constant) SetBool(x bool) {

	C.Constant_setBool(c.ptr, C.short(booltoshort(x)))

}

func (c *Constant) SetInt(x int32) {

	C.Constant_setInt(c.ptr, C.int(x))

}

func (c *Constant) SetShort(x int16) {

	C.Constant_setShort(c.ptr, C.short(x))

}

func (c *Constant) SetLong(x int64) {

	C.Constant_setLong(c.ptr, C.longlong(x))

}

func (c *Constant) SetFloat(x float32) {

	C.Constant_setFloat(c.ptr, C.float(x))

}

func (c *Constant) SetDouble(x float64) {

	C.Constant_setDouble(c.ptr, C.double(x))

}

func (c *Constant) SetString(x string) {

	C.Constant_setString(c.ptr, C.CString(x))

}

func (c *Constant) SetNull(x float64) {

	C.Constant_setNull(c.ptr)

}

func (c *Constant) SetByIndex(index int, val Constant) {

	C.Constant_setByIndex(c.ptr, C.int(index), val.ptr)

}

func DelConstant(c Constant) {

	C.delConstant(c.ptr)

}

func CreateConstant(typedol int) Constant {

	return Constant{C.createConstant(C.int(typedol))}
}

func (c *Constant) SetBinary(val []byte) {
	if len(val) != 16 {
		panic("bytes length must be 16")
	}
	C.Constant_setBinary(c.ptr, (*C.char)(unsafe.Pointer(&val[0])))
}

func (c *Constant) SetBinaryByIndex(index int, val []byte) {
	if len(val) != 16 {
		panic("bytes length must be 16")
	}
	C.Constant_setBinaryByIndex(c.ptr, C.int(index), (*C.char)(unsafe.Pointer(&val[0])))

}

func (c *Constant) SetBinaryArray(start int, l int, val []byte) bool {
	if len(val) != l*16 {
		panic("bytes length must be 16")
	}

	return tobool(C.Constant_setBinaryArray(c.ptr, C.int(start), C.int(l), (*C.char)(unsafe.Pointer(&val[0]))))

}

func ParseConstant(typedol int, val string) Constant {

	return Constant{C.parseConstant(C.int(typedol), C.CString(val))}
}

func (c *Constant) GetHash(buckets int) int {
	return int(C.Constant_getHash(c.ptr, C.int(buckets)))
}

func (c *Constant) GetHashArray(start int, l int, buckets int, buf []int32) bool {
	return tobool(C.Constant_getHashArray(c.ptr, C.int(start), C.int(l), C.int(buckets), (*C.int)(unsafe.Pointer(&buf[0]))))
}

func GetEpochTime() int64 {
	return int64(C.getEpochTime())
}

func main() {

	conn := new(DBConnection)
	conn.Init()
	fmt.Println(conn.Connect(hostname, port, user, pass))
	v1 := CreateVector(DT_INT, 0)
	v2 := CreateVector(DT_INT, 0)
	cols := []Vector{v1, v2}
	v1.Append(CreateInt(1))
	v2.Append(CreateInt(1))
	//chuan := [] int32 {1,2,3,4,5};
	//v1.AppendInt(chuan, 5);
	//fmt.Println(v1.GetString());
	//var tab1 Vector;

	set1 := conn.Run("set([5,5,3,4])")
	set2 := set1.ToSet()
	fmt.Println(set2.GetScript())
	fmt.Println("form is", set1.GetForm())

	v4 := CreateVector(DT_STRING, 0)
	s1 := []string{"12321", "21313", "asd"}
	v4.AppendString(s1, 3)
	fmt.Println(v4.GetString())
	s2 := []string{"1", "2", "3"}
	v4.SetStringArray(0, 3, s2)
	fmt.Println(v4.GetString())

	//conn.Upload("v4",v4.ToConstant());

	v5 := CreateVector(DT_BOOL, 5)
	b1 := []bool{true, true, true}
	v5.AppendBool(b1, 3)
	b2 := []bool{false, false, false, false, false}
	v5.SetBoolArray(0, 5, b2)
	fmt.Println(v5.GetString())
	//	v3 := CreateVector(DT_LONG,0);
	//	chuan1 := [] int64 {1,2,3,4,5};
	//	v3.AppendInt(chuan1);
	//	fmt.Println(v3.GetString());
	colnames := []string{"v1", "v2"}
	ta := CreateTableByVector(colnames, cols)
	fmt.Println(ta.GetString())
	dropcol := []int{0}
	ta.Drop(dropcol)
	fmt.Println(ta.GetString())

	v6 := CreateVector(DT_STRING, 5)
	v6.SetStringByIndex(1, "1111")
	v6.SetByIndex(2, CreateString("2222"))
	fmt.Println(v6.GetString())

	cdel := CreateInt(1)
	DelConstant(cdel)

	xn := ParseConstant(DT_INT, "1")
	fmt.Println(xn.GetString())

	uuid := CreateConstant(DT_IP)
	b := []byte{255, 255, 255, 1, 1, 1, 1, 1, 255, 255, 255, 1, 1, 1, 1, 1}
	bx := []byte{255, 255, 255, 1, 1, 1, 1, 1, 255, 255, 255, 1, 1, 1, 1, 1, 255, 255, 255, 1, 1, 1, 1, 1, 255, 255, 255, 1, 1, 1, 1, 1}
	uuid.SetBinary(b)
	fmt.Println(uuid.GetString())
	vu := CreateVector(DT_IP, 5)
	//ud := []string{"192.168.34.232", "192.168.34.232" ,"192.168.34.232" ,"192.168.34.232" ,"192.168.34.232"};

	//fmt.Println(vu.GetString());
	vu.SetBinaryByIndex(1, b)
	vu.SetBinaryArray(0, 2, bx)

	fmt.Println(vu.GetString())

	//	coltypes:= [] int{DT_INT,DT_INT};
	//	tb :=  CreateTable(colnames, coltypes, 10, 15)
	//	fmt.Println(tb.GetString());
	conn.Close()

}
