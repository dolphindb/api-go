/*
#include <cstring>
#include "DolphinDB.h"
#include "export.h"

*/

#include "Util.h"

#include "DolphinDB.h"


#include "Streaming.h"
#include <iostream>
#include <cstdio>
#include <cstring>
using namespace dolphindb;

extern "C" {
// typedef struct DBConnection DBConnection;
// typedef struct Constant Constant;
// typedef bool bool;

struct Wrapper {
  ConstantSP _internal;
};

struct WrapperMessageQueue  {
   MessageQueueSP _internal;
};

int  MessageQueue_poll(WrapperMessageQueue* w,  Wrapper* msg,  int s) {
     return w->_internal->poll(msg->_internal, s );
  }

void* Constant_new(){
    return new Wrapper;
}

int Constant_isNull(Wrapper* w)
{
  return w->_internal->isNull();

}

char* def_action_name(){
    return (char*)(DEFAULT_ACTION_NAME);
}


PollingClient* PollingClient_new(int listerport ) { return (new PollingClient(listerport));}

void* PollingClient_subscribe( PollingClient* client, char* host, int port, char*  tableName,char*  actionName ,   long long offset)
{
 
       return new WrapperMessageQueue{client->subscribe(host, port, tableName, actionName, offset)};

} 

void PollingClient_unsubscribe(PollingClient* client, char* host, int port, char* tableName, char* actionName){
   client->unsubscribe(host, port, tableName, actionName);
}

DBConnection* DBConnection_new() { return (new DBConnection()); }

int DBConnection_connect(DBConnection* conn, char* host, int port, char* user,
                         char* pass) {
  //  DBConnection* handle = (DBConnection *) conn;
  return conn->connect(host, port, user, pass);
}

void* DBConnection_run(DBConnection* conn, char* s) {
  //  DBConnection* handle = (DBConnection *) conn;
  Wrapper* wrapper = new Wrapper{conn->run(s)};
  return (void*)wrapper;
}

void DBConnection_upload(DBConnection* conn, char* name, Wrapper* w) {
  conn->upload(name, w->_internal);
}

// class Vector:public Constant{
void DBConnection_close(DBConnection* conn) { conn->close(); }

int Constant_getBool(Wrapper* w) { return w->_internal->getBool(); }

int Constant_getForm(Wrapper* w) {
  //  int b = c->isScalar();
  // int p = w->_internal->getInt();
  return w->_internal->getForm();
}

int Constant_getType(Wrapper* w) {
  //  int b = c->isScalar();
  // int p = w->_internal->getInt();
  return w->_internal->getType();
}

int Constant_getInt(Wrapper* w) {
  //  int b = c->isScalar();
  // int p = w->_internal->getInt();
  return w->_internal->getInt();
  // return 1;
}

char Constant_getChar(Wrapper* w) { return w->_internal->getChar(); }

short Constant_getShort(Wrapper* w) { return w->_internal->getShort(); }
long long Constant_getLong(Wrapper* w) { return w->_internal->getLong(); }
int Constant_getIndex(Wrapper* w) { return w->_internal->getIndex(); }

float Constant_getFloat(Wrapper* w) { return w->_internal->getFloat(); }
double Constant_getDouble(Wrapper* w) { return w->_internal->getDouble(); }

char* Constant_getString(Wrapper* w) {
  return (char*)(w->_internal->getString().data());
}

int Constant_isScalar(Wrapper* w) { return w->_internal->isScalar(); }
int Constant_isArray(Wrapper* w) { return w->_internal->isArray(); }

int Constant_isPair(Wrapper* w) { return w->_internal->isPair(); }

int Constant_isMatrix(Wrapper* w) { return w->_internal->isMatrix(); }

int Constant_isVector(Wrapper* w) { return w->_internal->isVector(); }

int Constant_isTable(Wrapper* w) { return w->_internal->isTable(); }

int Constant_isSet(Wrapper* w) { return w->_internal->isSet(); }

int Constant_isDictionary(Wrapper* w) { return w->_internal->isDictionary(); }

int Constant_size(Wrapper* w) { return (w->_internal->size()); }

struct WrapperVector {
  VectorSP _internal;
};

struct WrapperTable {
  TableSP _internal;
};
struct WrapperMatrix {
  MatrixSP _internal;
};
struct WrapperSet {
  SetSP _internal;
};
struct WrapperDictionary {
  DictionarySP _internal;
};

void* toConstant(Wrapper* w) {
  Wrapper* wrapper = new Wrapper{w->_internal};

  return (void*)wrapper;
}

void* toVector(Wrapper* w) {
  WrapperVector* wrapper = new WrapperVector{w->_internal};

  return (void*)wrapper;
}

void* toMatrix(Wrapper* w) {
  WrapperMatrix* wrapper = new WrapperMatrix{w->_internal};

  return (void*)wrapper;
}

void* toSet(Wrapper* w) {
  WrapperSet* wrapper = new WrapperSet{w->_internal};

  return (void*)wrapper;
}

void* toDictionary(Wrapper* w) {
  WrapperDictionary* wrapper = new WrapperDictionary{w->_internal};

  return (void*)wrapper;
}

void* toTable(Wrapper* w) {
  WrapperTable* wrapper = new WrapperTable{w->_internal};

  return (void*)wrapper;
}

void Set_clear(WrapperSet* w) { w->_internal->clear(); }

int Set_remove(WrapperSet* w, Wrapper* val) {
  return w->_internal->remove(val->_internal);
}

int Set_append(WrapperSet* w, Wrapper* val) {
  return w->_internal->append(val->_internal);
}

int Set_inverse(WrapperSet* w, Wrapper* val) {
  return w->_internal->inverse(val->_internal);
}

void Set_contain(WrapperSet* w, Wrapper* target, Wrapper* result) {
  w->_internal->contain(target->_internal, result->_internal);
}

int Set_isSuperSet(WrapperSet* w, Wrapper* target) {
  return w->_internal->isSuperset(target->_internal);
}

char* Set_getScript(WrapperSet* w) {
  return (char*)(w->_internal->getScript().data());
}

int Constant_isLargeConstant(Wrapper* w) {
  return w->_internal->isLargeConstant();
}

void* Set_interaction(WrapperSet* w, Wrapper* target) {
  return new Wrapper{w->_internal->interaction(target->_internal)};
}

void* Set_getSubVector(WrapperSet* w, int start, int length) {
  return new Wrapper{w->_internal->getSubVector(start, length)};
}

void Matrix_setRowLabel(WrapperMatrix* w, Wrapper* x) {
  w->_internal->setRowLabel(x->_internal);
}

void Matrix_setColumnLabel(WrapperMatrix* w, Wrapper* x) {
  w->_internal->setColumnLabel(x->_internal);
}

int Matrix_reshape(WrapperMatrix* w, int cols, int rows) {
  w->_internal->reshape(cols, rows);
}

void* Matrix_getColumn(WrapperMatrix* w, int x) {
  WrapperVector* wrapper = new WrapperVector{w->_internal->getColumn(x)};
  return (void*)wrapper;
}

char* Matrix_getStringbyIndex(WrapperMatrix* w, int x) {
  return (char*)(w->_internal->getString(x).data());
}

char* Matrix_getCellString(WrapperMatrix* w, int x, int y) {
  return (char*)(w->_internal->getString(x, y).data());
}

int Matrix_setColumn(WrapperMatrix* w, int index, Wrapper* col) {
  return w->_internal->setColumn(index, col->_internal);
}

void* Matrix_getInstance(WrapperMatrix* w, int size) {
  return new Wrapper{w->_internal->getInstance(size)};
}

int Dictionary_count(WrapperDictionary* w) { return w->_internal->count(); }

void Dictionary_clear(WrapperDictionary* w) { w->_internal->clear(); }

void* Dictionary_getMember(WrapperDictionary* w, Wrapper* key) {
  return new Wrapper{w->_internal->getMember(key->_internal)};
}

void* Dictionary_getMemberbyString(WrapperDictionary* w, char* key) {
  return new Wrapper{w->_internal->getMember(key)};
}

void* Dictionary_getCell(WrapperDictionary* w, int col, int row) {
  return new Wrapper{w->_internal->get(col, row)};
}

int Dictionary_getKeyType(WrapperDictionary* w) {
  return w->_internal->getKeyType();
}

void* Dictionary_keys(WrapperDictionary* w) {
  return new Wrapper{w->_internal->keys()};
}

void* Dictionary_values(WrapperDictionary* w) {
  return new Wrapper{w->_internal->values()};
}

// bool setColumn(INDEX index, const ConstantSP& value);

char* Dictionary_getScript(WrapperDictionary* w) {
  return (char*)(w->_internal->getScript().data());
}

int Dictionary_remove(WrapperDictionary* w, Wrapper* key) {
  return w->_internal->remove(key->_internal);
}

int Dictionary_set(WrapperDictionary* w, Wrapper* key, Wrapper* value) {
  return w->_internal->set(key->_internal, value->_internal);
}

int Dictionary_setbyString(WrapperDictionary* w, char* key, Wrapper* value) {
  return w->_internal->set(key, value->_internal);
}

void Dictionary_contain(WrapperSet* w, Wrapper* target, Wrapper* result) {
  w->_internal->contain(target->_internal, result->_internal);
}

void Table_setName(WrapperTable* w, char* tname) {
  w->_internal->setName(tname);
}

char* Table_getName(WrapperTable* w) {
  return (char*)(w->_internal->getName().data());
}

char* Table_getColumnName(WrapperTable* w, int x) {
  return (char*)(w->_internal->getColumnName(x).data());
}

void* Table_getColumn(WrapperVector* w, int x) {
  WrapperVector* wrapper = new WrapperVector{w->_internal->getColumn(x)};
  return (void*)wrapper;
}
void* Table_getColumnbyName(WrapperTable* w, char* s) {
  WrapperVector* wrapper = new WrapperVector{w->_internal->getColumn(s)};
  return (void*)wrapper;
}

int Table_columns(WrapperTable* w) { return w->_internal->columns(); }
int Table_rows(WrapperTable* w) { return w->_internal->rows(); }
int Table_getColumnType(WrapperTable* w, int x) {
  return w->_internal->getColumnType(x);
}

void* createInt(int val) { return new Wrapper{Util::createInt(val)}; }

void* createBool(int val) { return new Wrapper{Util::createBool((bool)val)}; }

void* createChar(char val) { return new Wrapper{Util::createInt(val)}; }

void* createShort(short val) { return new Wrapper{Util::createShort(val)}; }
void* createLong(long long val) { return new Wrapper{Util::createLong(val)}; }
void* createFloat(float val) { return new Wrapper{Util::createFloat(val)}; }
void* createDouble(double val) { return new Wrapper{Util::createDouble(val)}; }
void* createString(char* val) { return new Wrapper{Util::createString(val)}; }

void* createDate(int year, int month, int day) {
  return new Wrapper{Util::createDate(year, month, day)};
}

void* createMonth(int year, int month) {
  return new Wrapper{Util::createMonth(year, month)};
}

void* createNanoTime(int hour, int minute, int second, int nanosecond) {
  return new Wrapper{Util::createNanoTime(hour, minute, second, nanosecond)};
}

void* createTime(int hour, int minute, int second, int millisecond) {
  return new Wrapper{Util::createTime(hour, minute, second, millisecond)};
}

void* createSecond(int hour, int minute, int second) {
  return new Wrapper{Util::createSecond(hour, minute, second)};
}

void* createMinute(int hour, int minute) {
  return new Wrapper{Util::createMinute(hour, minute)};
}

void* createNanoTimestamp(int year, int month, int day, int hour, int minute,
                          int second, int nanosecond) {
  return new Wrapper{Util::createNanoTimestamp(year, month, day, hour, minute,
                                               second, nanosecond)};
}

void* createTimestamp(int year, int month, int day, int hour, int minute,
                      int second, int millisecond) {
  return new Wrapper{Util::createTimestamp(year, month, day, hour, minute,
                                           second, millisecond)};
}

void* createDateTime(int year, int month, int day, int hour, int minute,
                     int second) {
  return new Wrapper{
      Util::createDateTime(year, month, day, hour, minute, second)};
}

void* createVector(int type, int size = 0, int capacity = 0) {
  return new WrapperVector{Util::createVector((DATA_TYPE)type, size)};
}

void* DBConnection_runfunc(DBConnection* conn, char* script,
                           WrapperVector* args) {
  std::vector<ConstantSP> argv;
  for (int i = 0; i < args->_internal->size(); i++) {
    argv.push_back(args->_internal->get(i));
  }
  return new Wrapper{conn->run(script, argv)};
}

void* createTable(WrapperVector* colname, WrapperVector* coltypes, int size,
                  int capacity, int len) {
  // CString x;
  std::vector<string> names;
  std::vector<DATA_TYPE> colt;
  for (int i = 0; i < len; i++) {
    //     std::cout << i << std::endl;
    names.push_back(colname->_internal->get(i)->getString());
    //      ConstantSP k = cols[i]->_internal;
    //      std::cout << i << std::endl;
    // colt.push_back(DT_INT);
    // colt.push_back((DATA_TYPE)coltypes[i]);
    colt.push_back((DATA_TYPE)(coltypes->_internal->get(i)->getInt()));
    // coltypes++;
    //     cols += sizeof(cols);
  }

  return new WrapperTable{Util::createTable(names, colt, size, capacity)};
}

void* createTableByVector(WrapperVector* colname, WrapperVector* cols,
                          int len) {
  // CString x;
  std::vector<string> names;
  std::vector<ConstantSP> colptr;
  for (int i = 0; i < len; i++) {
    //     std::cout << i << std::endl;
    names.push_back(colname->_internal->get(i)->getString());
    //      ConstantSP k = cols[i]->_internal;
    //      std::cout << i << std::endl;

    colptr.push_back(cols->_internal->get(i));
    //     cols += sizeof(cols);
  }

  return new WrapperTable{Util::createTable(names, colptr)};
}

void Vector_setName(WrapperVector* w, char* vname) {
  w->_internal->setName(vname);
}

char* Vector_getName(WrapperVector* w) {
  return (char*)(w->_internal->getName().data());
}

void* Constant_get(Wrapper* w, int x) {
  return new Wrapper{w->_internal->get(x)};
}

int Vector_remove(WrapperVector* v, int x) { return v->_internal->remove(x); }
int Vector_append(WrapperVector* v, Wrapper* w) {
  return v->_internal->append(w->_internal);
}

int Vector_appendBool(WrapperVector* v, char* buf, int len) {
  return v->_internal->appendBool(buf, len);
}

int Vector_appendInt(WrapperVector* v, int* buf, int len) {
  return v->_internal->appendInt(buf, len);
}

int Vector_appendShort(WrapperVector* v, short* buf, int len) {
  return v->_internal->appendShort(buf, len);
}

int Vector_appendLong(WrapperVector* v, long long* buf, int len) {
  return v->_internal->appendLong(buf, len);
}

int Vector_appendFloat(WrapperVector* v, float* buf, int len) {
  return v->_internal->appendFloat(buf, len);
}

int Vector_appendDouble(WrapperVector* v, double* buf, int len) {
  return v->_internal->appendDouble(buf, len);
}

int Vector_appendString(WrapperVector* v, char* buf, int len) {
  return v->_internal->appendString(&buf, len);
}

void* Vector_getColumnLabel(WrapperVector* w) {
  return new Wrapper{w->_internal->getColumnLabel()};
}

int Vector_isView(WrapperVector* w) { return w->_internal->isView(); }

void Vector_initialize(WrapperVector* w) { w->_internal->initialize(); }

int Vector_getCapacity(WrapperVector* w) { return w->_internal->getCapacity(); }

int Vector_reserve(WrapperVector* w, int x) { return w->_internal->reserve(x); }

int Vector_getUnitLength(WrapperVector* w) {
  return w->_internal->getUnitLength();
}

void Vector_clear(WrapperVector* w) { w->_internal->clear(); }

int Vector_removebyIndex(WrapperVector* w, Wrapper* index) {
  return w->_internal->remove(index->_internal);
}

void* Vector_getInstance(WrapperVector* w, int size) {
  return new Wrapper{w->_internal->getInstance(size)};
}

void* Vector_getSubVector(WrapperVector* w, int start, int l) {
  return new Wrapper{w->_internal->getSubVector(start, l)};
}

void Vector_fill(WrapperVector* w, int start, int l, Wrapper* val) {
  w->_internal->fill(start, l, val->_internal);
}

void Vector_next(WrapperVector* w, int steps) { w->_internal->next(steps); }

void Vector_prev(WrapperVector* w, int steps) { w->_internal->prev(steps); }

void Vector_reverse(WrapperVector* w) { w->_internal->reverse(); }

void Vector_reverseSegment(WrapperVector* w, int start, int l) {
  w->_internal->reverse(start, l);
}

void Vector_replace(WrapperVector* w, Wrapper* oldval, Wrapper* newval) {
  w->_internal->replace(oldval->_internal, newval->_internal);
}

int Vector_validIndex(WrapperVector* w, int index) {
  return w->_internal->validIndex(index);
}

void Vector_addIndex(WrapperVector* w, int start, int l, int offset) {
  w->_internal->addIndex(start, l, offset);
}

void Vector_neg(WrapperVector* w) { w->_internal->neg(); }

char* Table_getScript(WrapperTable* w) {
  return (char*)(w->_internal->getScript().data());
}

char* Table_getColumnQualifier(WrapperTable* w, int index) {
  return (char*)(w->_internal->getColumnQualifier(index).data());
}

void Table_setColumnName(WrapperTable* w, int index, char* name) {
  w->_internal->setColumnName(index, name);
}

int Table_getColumnIndex(WrapperTable* w, char* name) {
  return w->_internal->getColumnIndex(name);
}

int Table_contain(WrapperTable* w, char* name) {
  return w->_internal->contain(name);
}

void* Table_getValue(WrapperTable* w) {
  return new Wrapper{w->_internal->getValue()};
}

void* Table_getInstance(WrapperTable* w, int size) {
  return new Wrapper{w->_internal->getInstance(size)};
}

int Table_sizeable(WrapperTable* w) { return w->_internal->sizeable(); }

char* Table_getStringbyIndex(WrapperTable* w, int index) {
  return (char*)(w->_internal->getString(index).data());
}

void* Table_getWindow(WrapperTable* w, int colStart, int colLength,
                      int rowStart, int rowLength) {
  return new Wrapper{
      w->_internal->getWindow(colStart, colLength, rowStart, rowLength)};
}

void* Table_getMember(WrapperTable* w, Wrapper* key)

{
  return new Wrapper{w->_internal->getMember(key->_internal)};
}

void* Table_values(WrapperTable* w)

{
  return new Wrapper{w->_internal->values()};
}

void* Table_keys(WrapperTable* w)

{
  return new Wrapper{w->_internal->keys()};
}

int Table_getTableType(WrapperTable* w) { return w->_internal->getTableType(); }

void Table_drop(WrapperTable* w, WrapperVector* v) {
  vector<int> dropvec;
  int l = v->_internal->size();
  for (int i = 0; i < l; i++) {
    dropvec.push_back(v->_internal->get(i)->getInt());
  }
  w->_internal->drop(dropvec);
}

int Constant_setBoolArray(Wrapper* w, int start, int len, char* buf) {
  return w->_internal->setBool(start, len, buf);
}

int Constant_setIntArray(Wrapper* w, int start, int len, int* buf) {
  return w->_internal->setInt(start, len, buf);
}

int Constant_setLongArray(Wrapper* w, int start, int len, long long* buf) {
  return w->_internal->setLong(start, len, buf);
}

int Constant_setShortArray(Wrapper* w, int start, int len, short* buf) {
  return w->_internal->setShort(start, len, buf);
}

int Constant_setFloatArray(Wrapper* w, int start, int len, float* buf) {
  return w->_internal->setFloat(start, len, buf);
}

int Constant_setDoubleArray(Wrapper* w, int start, int len, double* buf) {
  return w->_internal->setDouble(start, len, buf);
}

int Constant_setStringArray(Wrapper* w, int start, int len, char* buf) {
  return w->_internal->setString(start, len, &buf);
}

void Constant_setIntByIndex(Wrapper* w, int index, int val) {
  w->_internal->setInt(index, val);
}

void Constant_setBoolByIndex(Wrapper* w, int index, short val) {
  w->_internal->setBool(index, (bool)val);
}

void Constant_setShortByIndex(Wrapper* w, int index, short val) {
  w->_internal->setShort(index, val);
}

void Constant_setLongByIndex(Wrapper* w, int index, long long val) {
  w->_internal->setLong(index, val);
}

void Constant_setFloatByIndex(Wrapper* w, int index, float val) {
  w->_internal->setFloat(index, val);
}

void Constant_setDoubleByIndex(Wrapper* w, int index, double val) {
  w->_internal->setDouble(index, val);
}

void Constant_setStringByIndex(Wrapper* w, int index, char* val) {
  w->_internal->setString(index, val);
}

void Constant_setNullByIndex(Wrapper* w, int index) {
  w->_internal->setNull(index);
}

int Constant_setByIndex(Wrapper* w, int index, Wrapper* val) {
  return w->_internal->set(index, val->_internal);
}

void Constant_setInt(Wrapper* w, int val) { w->_internal->setInt(val); }
void Constant_setBool(Wrapper* w, short val) {
  w->_internal->setBool((bool)val);
}
void Constant_setShort(Wrapper* w, short val) { w->_internal->setInt(val); }
void Constant_setLong(Wrapper* w, long long val) { w->_internal->setLong(val); }
void Constant_setFloat(Wrapper* w, float val) { w->_internal->setFloat(val); }
void Constant_setDouble(Wrapper* w, double val) {
  w->_internal->setDouble(val);
}
void Constant_setString(Wrapper* w, char* val) { w->_internal->setString(val); }

void Constant_setNull(Wrapper* w) { w->_internal->setNull(); }

void delConstant(Wrapper* w) { delete (w); }
void delVector(WrapperVector* w) { delete w; }
void delTable(WrapperTable* w) { delete w; }
void delMatrix(WrapperMatrix* w) { delete w; }
void delSet(WrapperSet* w) { delete w; }
void delDictionary(WrapperDictionary* w) { delete w; }

void* createConstant(int type) {
  return new Wrapper{Util::createConstant((DATA_TYPE)type)};
}

void Constant_setBinary(Wrapper* w, unsigned char* val) {
  w->_internal->setBinary((const unsigned char*)val, 16);
}

void Constant_setBinaryByIndex(Wrapper* w, int index, unsigned char* val) {
  w->_internal->setBinary(index, 16, (const unsigned char*)val);
}

//	virtual bool setBinary(INDEX start, int len, int unitLength, const
// unsigned char* buf){return false;}

int Constant_setBinaryArray(Wrapper* w, int start, int len,
                            unsigned char* buf) {
  return w->_internal->setBinary(start, len, 16, (const unsigned char*)buf);
}

void* parseConstant(int type, char* word) {
  return new Wrapper{Util::parseConstant(type, word)};
}

int Constant_getHash(Wrapper* w, int buckets) {
  return w->_internal->getHash(buckets);
}

int Constant_getHashArray(Wrapper* w, int start, int len, int buckets,
                          int* buf) {
  return w->_internal->getHash(start, len, buckets, buf);
}

long long getEpochTime() { return Util::getEpochTime(); }
}
/* void* createTable(char** colname,Wrapper** cols,int len)
{
    //CString x;
    std::vector<string> names;
    std::vector<ConstantSP> colptr;
    for (int i=0;i<1;i++)
      {
     //     std::cout << i << std::endl;
          names.push_back((string)colname[i]);
    //      ConstantSP k = cols[i]->_internal;
    //      std::cout << i << std::endl;

          colptr.push_back(cols[i]->_internal);
    //     cols += sizeof(cols);
      }

    return new WrapperTable{Util::createTable(names,colptr)};
}
*/
/*
bool Vector_appendBool(WrapperVector* v,char* buf, int len)
{
    return false;}

        virtual bool appendChar(char* buf, int len){return false;}
        virtual bool appendShort(short* buf, int len){return false;}
        virtual bool appendInt(int* buf, int len){return false;}
        virtual bool appendLong(long long* buf, int len){return false;}
        virtual bool appendIndex(INDEX* buf, int len){return false;}
        virtual bool appendFloat(float* buf, int len){return false;}
        virtual bool appendDouble(double* buf, int len){return false;}
        virtual bool appendString(string* buf, int len){return false;}


}
*/

/*struct Wrapper {
    ConstantSP _internal;
};

void* DBConnection_run(DBConnection* conn, char* s)
{
  //  DBConnection* handle = (DBConnection *) conn;
    Wrapper *wrapper = new Wrapper{conn->run(s)};
    return (void *) wrapper;
} */

int main() {
  DBConnection conn;
  conn.connect("localhost", 1621);
  SetSP x = conn.run("set(8 9 4 6);");
  // SetSP y = x;
  // SetSP y = (SetSP) x;
  ConstantSP p = conn.run("set(7 10);");
  // std::cout<< (x->inverse(p)) << std::endl;
  // std::cout<< (x->getForm()) << std::endl;
  DictionarySP a = conn.run("x=1 2 3 1;y=2.3 4.6 5.3 6.4;dict(x, y)");
  VectorSP v = conn.run("1 2 3");
  // ConstantSP v1 = v->getValue(5);
  v->addIndex(1, 5, 5);
  // std::cout<< (v->getString()) << std::endl;

  TableSP t = conn.run("table(1 2 3 as a, `x`y`z as b, 10.8 7.6 3.5 as c)");
  std::cout << (t->keys()->getString()) << std::endl;
  vector<int> dropcol;
  dropcol.push_back(1);
  t->drop(dropcol);
  std::cout << (t->getString()) << std::endl;

  ConstantSP x1 = Util::createInt(99);
  std::cout << x1->getHash(5) << std::endl;
  // std::cout<< (v->reserve(2)) << std::endl;
  // ConstantSP m = conn.run("1..10$2:5");
  // std::cout<< (m->getForm()) << std::endl;
  // std::cout<< (v->getColumnLabel()->getString()) << std::endl;
  // std::cout<< (a->getString(1)) << std::endl;
  // set(8 9 9 4 6);
  /*
    Wrapper* a[2];
    a[0] =new Wrapper{Util::createVector(DT_INT,0)};
    a[1]= new  Wrapper{Util::createVector(DT_INT,0)};
    char s1[10] = "a";
    char s2[10] = "b";
    char* b[2] = {s1,s2};
    WrapperTable* t = (WrapperTable*)createTable(b,a,2);
    std::cout << t->_internal->getString() << std::endl;
    return 0;
  */
 /*
  const string host = "localhost";
const int port = 1621;
    int listenport = 1029;
    PollingClient client(listenport);

    auto queue = client.subscribe(host, port, "st1", DEFAULT_ACTION_NAME, 0);
    Message msg;
    while (true) {
        if (queue->poll(msg, 1000)) {
            if  (msg->isNull()) break;
            std::cout<< (msg->getString()) <<std::endl;
        }
    }
    */
  return 0;
}