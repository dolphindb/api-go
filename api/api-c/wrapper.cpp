/*
#include "export.h"
#include <cstring>
#include "DolphinDB.h"

// extern "C" {
// typedef struct DBConnection DBConnection;
// typedef struct Constant Constant;
// DBConnection* ddb_create_connection();
// int ddb_connect(const char* host, int port, const char* user, const char* password, const char* startup, int highAvailiablity);
// int ddb_login(const char* userId, const char* password, int enableEncryption);
// Constant* ddb_run1(const char* script, int priority=4, int parallelism=2);
// Constant* ddb_run2(const char* funcName, Constant* args, int size, int priority=4, int parallelism=2);
// void ddb_upload(const char* name, Constant* obj, int size);
// void ddb_upload2(char** names, Constant* objs, int size);
// void ddb_close(DBConnection* conn);
// void ddb_initialize();
//}

extern "C" {
DBConnection* ddb_create_connection() {
    return reinterpret_cast<DBConnection*>(new dolphindb::DBConnection);
}
int ddb_connect(DBConnection* conn, const char* host, int port, const char* user, const char* password, const char* startup, int highAvailiablity) {
    auto c = reinterpret_cast<dolphindb::DBConnection*>(conn);
    return c->connect(host, port, user, password, startup, highAvailiablity);
}
void ddb_login(DBConnection* conn, const char* userId, const char* password, int enableEncryption) {
    auto c = reinterpret_cast<dolphindb::DBConnection*>(conn);
    c->login(userId, password, enableEncryption);
}
Constant* ddb_run1(DBConnection* conn, const char* script, int priority, int parallelism) {
    auto c = reinterpret_cast<dolphindb::DBConnection*>(conn);
    auto ret = c->run(script, priority, parallelism);
    return reinterpret_cast<Constant*>(ret.release());
}
Constant* ddb_run2(DBConnection* conn, const char* funcName, Constant** args, int size, int priority, int parallelism) {
    vector<dolphindb::ConstantSP> args2(size);
    for (int i = 0; i < size; ++i) {
        args2[i] = reinterpret_cast<dolphindb::Constant*>(args[i]);
    }
    auto c = reinterpret_cast<dolphindb::DBConnection*>(conn);
    auto ret = c->run(funcName, args2, priority, parallelism);
    return reinterpret_cast<Constant*>(ret.release());
}

void ddb_upload(DBConnection* conn, const char* name, Constant* obj) {
    auto c = reinterpret_cast<dolphindb::DBConnection*>(conn);
    c->upload(name, reinterpret_cast<dolphindb::Constant*>(obj));
}

void ddb_upload2(DBConnection* conn, char** names, Constant** objs, int size) {
    vector<dolphindb::ConstantSP> args2(size);
    vector<string> names_(size);
    for (int i = 0; i < size; ++i) {
        args2[i] = reinterpret_cast<dolphindb::Constant*>(objs[i]);
        names_[i] = names[i];
    }
    auto c = reinterpret_cast<dolphindb::DBConnection*>(conn);
    c->upload(names_, args2);
}
void ddb_close(DBConnection* conn);
void ddb_initialize() {
    dolphindb::DBConnection::initialize();
}
}

extern "C" {
const char* ddb_getString(Constant* val) {
    string s = reinterpret_cast<dolphindb::Constant*>(val)->getString();
    int size = s.size();
    char* buf = new char[size + 1];
    memcpy(buf, s.c_str(), size);
    buf[size] = '\0';
    return buf;
}
}

*/

#include "Util.h"

#include "DolphinDB.h"

#include <cstring>

using namespace dolphindb;


extern "C"
{
//typedef struct DBConnection DBConnection;
//typedef struct Constant Constant;
//typedef bool bool;

struct Wrapper {
    ConstantSP _internal;
};



DBConnection* DBConnection_new()
{
   
    return (new DBConnection());
   
}

int DBConnection_connect(DBConnection* conn, char* host, int port, char* user, char* pass ) 
{
  //  DBConnection* handle = (DBConnection *) conn;
   return  conn->connect(host, port, user, pass);

}

void* DBConnection_run(DBConnection* conn, char* s)
{
  //  DBConnection* handle = (DBConnection *) conn;
    Wrapper *wrapper = new Wrapper{conn->run(s)};
    return (void *) wrapper;
}


void DBConnection_upload(DBConnection* conn, char* name, Wrapper* w)
{

  conn->upload(name, w->_internal);

}








//class Vector:public Constant{
void DBConnection_close(DBConnection* conn){
    conn->close();
}

int Constant_getBool(Wrapper* w){



   return w->_internal->getBool();


}

int Constant_getForm(Wrapper* w){
//  int b = c->isScalar();
 // int p = w->_internal->getInt();
   return w->_internal->getForm();

}

int Constant_getType(Wrapper* w){
//  int b = c->isScalar();
 // int p = w->_internal->getInt();
   return w->_internal->getType();

}

int Constant_getInt(Wrapper* w){
//  int b = c->isScalar();
 // int p = w->_internal->getInt();
  return w->_internal->getInt();
  //return 1;
}

char Constant_getChar(Wrapper* w){



   return w->_internal->getChar();


}

short Constant_getShort(Wrapper* w){

   return w->_internal->getShort();
   
}
long long Constant_getLong(Wrapper* w){

   return w->_internal->getLong();
   
}
int Constant_getIndex(Wrapper* w){

   return w->_internal->getIndex();
   
}

float Constant_getFloat(Wrapper* w){

   return w->_internal->getFloat();
   
}
double Constant_getDouble(Wrapper* w){

   return w->_internal->getDouble();
   
}

char* Constant_getString(Wrapper* w){

   return (char*)(w->_internal->getString().data());
   
}

 int Constant_isScalar(Wrapper* w)  { 
     return w->_internal->isScalar();
     }
 int Constant_isArray(Wrapper* w)  { 
     return w->_internal->isArray();
     }
 int Constant_isPair(Wrapper* w)  { 
     return w->_internal->isPair();
     }
 int Constant_isMatrix(Wrapper* w)  { 
     return w->_internal->isMatrix();
     }
 int Constant_isVector(Wrapper* w)  { 
     return w->_internal->isVector();
     }
 int Constant_isTable(Wrapper* w)  { 
     return w->_internal->isTable();
     }
 int Constant_isSet(Wrapper* w)  { 
     return w->_internal->isSet();
     } 
 int Constant_isDictionary(Wrapper* w)  { 
     return w->_internal->isDictionary();
     }
int Constant_size(Wrapper* w){

   return (w->_internal->size());
   
}

/*struct Wrapper {
    ConstantSP _internal;
};

void* DBConnection_run(DBConnection* conn, char* s)
{
  //  DBConnection* handle = (DBConnection *) conn;
    Wrapper *wrapper = new Wrapper{conn->run(s)};
    return (void *) wrapper;
} */
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

void*  toConstant(Wrapper* w)
{

   Wrapper * wrapper = new Wrapper{w->_internal};

   return (void *)wrapper;


}


void*  toVector(Wrapper* w)
{

   WrapperVector * wrapper = new WrapperVector{w->_internal};

   return (void *)wrapper;


}

void*  toMatrix(Wrapper* w)
{

   WrapperMatrix * wrapper = new WrapperMatrix{w->_internal};

   return (void *)wrapper;


}

void*  toSet(Wrapper* w)
{

   WrapperSet * wrapper = new WrapperSet{w->_internal};

   return (void *)wrapper;


}

void*  toDictionary(Wrapper* w)
{

   WrapperDictionary * wrapper = new WrapperDictionary{w->_internal};

   return (void *)wrapper;


}


void*  toTable(Wrapper* w)
{

   WrapperTable * wrapper = new WrapperTable{w->_internal};

   return (void *)wrapper;


}

void Table_setName(WrapperTable* w, char* tname)
{
    w->_internal->setName(tname);

}

char* Table_getName(WrapperTable* w){

   return (char*)(w->_internal->getName().data());
   
}

char* Table_getColumnName(WrapperTable* w,int x){

   return (char*)(w->_internal->getColumnName(x).data());
   
}

void* Table_getColumn(WrapperTable* w,int x)
{
   WrapperVector * wrapper = new WrapperVector{w->_internal->getColumn(x)};
   return (void *)wrapper;
}
void* Table_getColumnbyName(WrapperTable* w,char* s)
{
   WrapperVector * wrapper = new WrapperVector{w->_internal->getColumn(s)};
   return (void *)wrapper;
}

int Table_columns(WrapperTable* w)
{
    return w->_internal->columns();
}
int Table_rows(WrapperTable* w)
{
    return w->_internal->rows();
}
int Table_getColumnType(WrapperTable* w,int x)
{
    return w->_internal->getColumnType(x);
}

void* createInt(int val){

    return new Wrapper{Util::createInt(val)};
}

void* createBool(int val){

    return new Wrapper{Util::createBool((bool)val)};
}

void* createChar(char val){

    return new Wrapper{Util::createInt(val)};
}

void* createShort(short val){

    return new Wrapper{Util::createShort(val)};
}
void* createLong(long long val){

    return new Wrapper{Util::createLong(val)};
}
void* createFloat(float val){

    return new Wrapper{Util::createFloat(val)};
}
void* createDouble(double val){

    return new Wrapper{Util::createDouble(val)};
}
void* createString(char* val){

    return new Wrapper{Util::createString(val)};
}


void* createDate(int year, int month, int day){

    return new Wrapper{Util::createDate(year, month, day)};
}

void* createMonth(int year, int month){
    
    return new Wrapper{Util::createMonth(year, month)};
}

void* createNanoTime(int hour, int minute, int second, int nanosecond){
    return new Wrapper{Util::createNanoTime(hour, minute, second, nanosecond)};
}

void* createTime(int hour, int minute, int second, int millisecond){
    return new Wrapper{Util::createTime(hour, minute, second, millisecond)};
}

void* createSecond(int hour, int minute, int second){
    return new Wrapper{Util::createSecond(hour, minute, second)};
}

void* createMinute(int hour, int minute){
    return new Wrapper{Util::createMinute(hour, minute)};
}

void* createNanoTimestamp(int year, int month, int day, int hour, int minute, int second, int nanosecond){

   return new Wrapper{Util::createNanoTimestamp(year, month, day, hour, minute, second, nanosecond)};
}

void* createTimestamp(int year, int month, int day, int hour, int minute, int second, int millisecond){

   return new Wrapper{Util::createTimestamp(year, month, day, hour, minute, second, millisecond)};
}

void* createDateTime(int year, int month, int day, int hour, int minute, int second){

   return new Wrapper{Util::createDateTime(year, month, day, hour, minute, second)};
}



void* createVector(int type, int size=0, int capacity=0){

    return new WrapperVector{Util::createVector((DATA_TYPE)type, 0)};
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
void* DBConnection_runfunc(DBConnection* conn, char* script, WrapperVector* args)
{
  std::vector<ConstantSP> argv;
  for (int i=0;i< args->_internal->size();i++)
    {
       argv.push_back(args->_internal->get(i));

    }
  return new Wrapper{conn->run(script,argv)};
}


 void* createTable(WrapperVector* colname,WrapperVector* cols,int len)
{   
    //CString x;
    std::vector<string> names;
    std::vector<ConstantSP> colptr;
    for (int i=0;i<len;i++)
      {
     //     std::cout << i << std::endl;
          names.push_back(colname->_internal->get(i)->getString());
    //      ConstantSP k = cols[i]->_internal;
    //      std::cout << i << std::endl;
          
          colptr.push_back(cols->_internal->get(i));
    //     cols += sizeof(cols);
      }
      
    return new WrapperTable{Util::createTable(names,colptr)};
}




void Vector_setName(WrapperVector* w, char* vname)
{
    w->_internal->setName(vname);

}

char* Vector_getName(WrapperVector* w)
{
   
 
   return (char*)(w->_internal->getName().data());


}

void* Vector_get(WrapperVector* w, int x)
{
   
 
   return new Wrapper{w->_internal->get(x)};


}

int Vector_remove(WrapperVector* v,int x)
{
   
 
   return v->_internal->remove(x);


}
int Vector_append(WrapperVector* v,Wrapper* w)
{
   
 
   return v->_internal->append(w->_internal);


}













}
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
/*
int main()
{

  Wrapper* a[2];
  a[0] =new Wrapper{Util::createVector(DT_INT,0)};
  a[1]= new  Wrapper{Util::createVector(DT_INT,0)};
  char s1[10] = "a";
  char s2[10] = "b";
  char* b[2] = {s1,s2};
  WrapperTable* t = (WrapperTable*)createTable(b,a,2);
  std::cout << t->_internal->getString() << std::endl;
  return 0;

}
*/