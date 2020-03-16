# DolphinDB Go API

DolphinDB Go API 目前仅支持Linux开发环境。

本教程主要介绍以下内容：

- 项目编译
- 建立DolphinDB连接
- 运行DolphinDB脚本
- 运行DolphinDB函数
- 数据对象介绍
- 上传本地对象到DolphinDB服务器
- 读写DolphinDB数据表

### 1.项目编译

#### 1.1 添加环境变量

下载整个项目，进入api-go目录，使用如下指令添加环境变量。请注意，执行export指令只能临时添加环境变量，若需要让变量持久生效，请根据Linux相关教程修改系统文件。

```bash
$ cd api-go/
$ export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/src
```

#### 1.2 导入API包

新建.go文件并导入DolphinDB GO API包，可参考api-go目录下example.go文件，包名已经简写为ddb。

```GO
package main
import (
	 "./src"
)
func main() {
  var conn ddb.DBConnection;
  conn.Init();
  conn.Connect("127.0.0.1",8920,"admin","123456");
}
```

### 2. 建立DolphinDB连接

DolphinDB GO API 提供的最核心的对象是DBConnection。GO应用可以通过它在DolphinDB服务器上执行脚本和函数，并在两者之间双向传递数据。DBConnection类提供如下主要方法：

| 方法名        | 详情          |
|:------------- |:-------------|
|Connect(host, port, username, password)|将会话连接到DolphinDB服务器|
|Run(script)|将脚本在DolphinDB服务器运行|
|RunFunc(functionName,args)|调用DolphinDB服务器上的函数|
|Upload(variableObjectMap)|将本地数据对象上传到DolphinDB服务器|
|Init()|初始化链接信息|
|Close()|关闭当前会话|

如下脚本声明了一个`DBConnection`对象，并调用`Init`方法初始化对象。请注意，GO API 在定义DBConnection对象之后必须首先调用`Init`方法来进行初始化配置，否则会导致API的一些方法不能正常使用。

```GO
var conn DBConnection;
conn.Init();
```

GO API通过TCP/IP协议连接到DolphinDB。使用`Connect`方法创建连接时，需要提供DolphinDB Server的IP、端口号、用户名及密码，函数返回一个布尔值表示是否连接成功。

```GO
conn.Connect("127.0.0.1",8848,"admin","123456");
```

### 3. 运行DolphinDB脚本

通过`Run`方法运行DolphinDB脚本：

```GO
v := conn.Run("`IBM`GOOG`YHOO");
fmt.Println(v.GetString());
```
输出结果为：
>["IBM","GOOG","YHOO"]

当需要调用DolphinDB内置或用户自定义函数时，若函数所需参数都在服务端，我们可以通过`Run`方法直接调用该函数。

例如，对两个向量调用[add](http://www.dolphindb.cn/cn/help/add.html)函数时，若函数所需的两个参数x和y都在服务端被定义，则直接调用`Run`：

```GO
sum := conn.Run("x = [1,3,5]; y = [2,4,6]; add(x,y)");
fmt.Println(sum.GetString());
```
输出结果为：
>[3,7,11]

### 4. 运行函数

当需要在远程DolphinDB服务器上执行DolphinDB内置或用户自定义函数，而函数所需的一个或多个参数需要由GO客户端提供时，我们可以通过`RunFunc`方法来调用这类函数。`RunFunc`的第一个参数为DolphinDB中的函数名，第二个参数是该函数所需的一个或者多个参数，为Constant类型的向量。下面仍以[add](http://www.dolphindb.cn/cn/help/add.html)函数为例，区分两种情况：

- 仅部分参数需由GO客户端赋值

若变量x已经通过GO程序在服务器端生成，

```GO
conn.Run("x = [1,3,5]");
```

而参数y要在GO客户端生成，这时就需要使用“部分应用”方式，把参数x固化在[add](http://www.dolphindb.cn/cn/help/add.html)函数内。具体请参考[部分应用文档](https://www.dolphindb.cn/cn/help/PartialApplication.html)。

```GO
a2 := [] int32 {9,8,7};
y0 := ddb.CreateVector(ddb.DT_INT, 3);
y0.SetIntArray(0,3,a2);
y := y0.ToConstant();
args := [] ddb.Constant{y};
result1 := conn.RunFunc("add{x,}", args);
fmt.Println(result1.GetString());
```
输出结果为：
> [10, 11, 12]

* 所有参数都待由GO客户端赋值

当所有参数都待由GO客户端赋值时，直接通过`RunFunc`方法调用DolphinDB的内置函数：

```GO
a1 := [] int32 {1,2,3};
a2 := [] int32 {9,8,7};
x0 := ddb.CreateVector(ddb.DT_INT, 3);
y0 := ddb.CreateVector(ddb.DT_INT, 3);
x0.SetIntArray(0,3,a1);
y0.SetIntArray(0,3,a2);
x := x0.ToConstant();
y := y0.ToConstant();
args = [] ddb.Constant{x, y};
result1 := conn.RunFunc("add", args);
fmt.Println(result1.GetString());
```
输出结果为：
>[10,10,10]

上述例子中，我们使用GO API中的`CreateVector`函数分别创建两个向量，再调用`SetIntArray`函数将GO语言中int类型的切片赋值给ddb.DT_INT类型的向量。最后调用`ToConstant`函数将vector转换成Constant对象，作为参数上传到DolphinDB server端。

### 5. 数据对象介绍

DolphinDB GO API 通过Constant这一基本类型接受各种类型的数据，包括DT_INT、DT_FLOAT等。同时，GO API还提供Vector类和Table类来存放向量和表对象。

#### 5.1 Constant类

Constant类提供的较为常用的方法如下：

| 方法名        | 详情          |
|:------------- |:-------------|
|Create\<DataType>|构造DataType类型的常量，参数为该常量的值|
|CreateConstant()|构造一个常量，参数为常量对象的数据类型|
|GetForm()|获取对象的数据形式|
|GetType()|获取对象的数据类型|
|GetHash(buckets int)|返回一个Constant的哈希值(mod buckets)|
|Size()|获取对象大小|
|Get\<DataType>|将DataType类型的常量转换为GO中对应的基本数据类型|
|Is\<DataForm>|判断常量的数据形式是否为DataForm|
|<Set\<DataType>|对DataType类型的常量值进行赋值，参数为修改之后的值|
|ParseConstant(DT_type int, val string)|将字符串val解析为DT_type类型，并返回一个对应类型的常量|
|ToVector()|转换为Vector类型|
|ToTable()|转换为Table类型|

具体示例如下：

* Create\<DataType>，CreateConstant()，Set\<DataType>

`Create<DataType>`和`CreateConstant`都是用于创建对应数据类型常量的方法，例如，创建一个值为"astr"字符串常量,有以下两种方式：

```Go
str1 := ddb.CreateString("astr");
fmt.Println(str1.GetString());

str2 := ddb.CreateConstant(ddb.DT_STRING);
str2.SetString("astr");  
```

需要注意的是，目前对于16字节的字符串类型（包括DT_UUID，DT_IP和DT_INT128），还不支持直接通过`Create<DataType>`的方式直接创建DataType类型的常量，需要先通过`CreateConstant`方法创建常量对象，再通过`SetBinary`方法赋值。

```GO
vuuid := ddb.CreateConstant(ddb.DT_UUID);
vipaddr := ddb.CreateConstant(ddb.DT_IP);
vint128 := ddb.CreateConstant(ddb.DT_INT128);

b := []byte{255, 255, 255, 1,1,1,1,1, 255, 255, 255, 1,1,1,1,1};
vuuid.SetBinary(b);
vipaddr.SetBinary(b);
vint128.SetBinary(b);
```

>请注意：在调用`SetBinary`方法为一个16字节字符串类型常量赋值时，参数必须是一个长度为16的byte类型数组，数组的每一位（取值范围为0~255）对应16字节字符串的每个字节。

* ParseConstant 

下例中，通过指定DT_type为DT_INT，val为“1”，将字符串“1”转变为int类型的常量对象。

```GO
xn := ddb.ParseConstant(DT_INT,"1");
```

该函数可以将一个字符串转化为16字节的字符串进行存储。下面的例子中，xn为一个DT_INT128类型的字符串。

```GO
xn := ddb.ParseConstant(ddb.DT_INT128,"08b80e4f20171412130ec0899884fef4");
```

* GetForm、GetType

对Constant对象调用`GetForm`方法获取对象的数据形式，调用`GetType`方法获取对象的数据类型。需要注意的是，这两个方法返回的不是字符串，而是数据形式或者数据类型对应的序号，具体对应关系见附录。

```GO
x := conn.Run("1+1");
x.GetForm();
x.GetType();
```
输出结果为如下，其中，form=0代表是form为scalar，type=4代表type为int。

>0 4

* GetHash

对常量对象调用GetHash方法，会对该常量做mod buckets运算，并返回运算结果。

* Size

Size方法可以获取对象大小，对于Vector会获取长度，对于Table会获取行数

```GO
p.Size();
```

* Get\<DataType>

通过`Get<DataType>`系列方法，将Constant对象转换为GO语言中的常用类型

```GO
x := conn.Run("1+1");
x.GetBool(); //转换为布尔型
x.GetShort();  //int16
x.GetInt();  //转换为整形int
x.GetLong();  //int64
x.GetFloat(); //float32
x.GetDouble();  //float64
x.GetString()  //转换为字符串
```

* Is\<DataForm>

使用`Is<DataForm>`系列方法，校验对象的数据形式

```GO
x := conn.Run("2 3 5");
x.IsScalar();
x.IsVector();
x.IsTable();
```

对Constant对象调用`ToVector`可以获得一个Vector对象，Vector类的介绍见5.2小节。

```GO
p := conn.Run("5 4 8");
p1 := p.ToVector();
```

类似地，对Constant对象调用`ToTable`可以获得一个Table对象, Table类的介绍见5.3小节。

```GO
script := "t=table(1..5 as id, rand(5.0, 5) as values);"
script += "select * from t";
p := conn.Run(script);
p1 := p.ToTable();
```

#### 5.2 Vector类

Vector(向量)是DolphinDB中常用的类型，也可作为表中的一列，Vector类提供的较为常用的方法如下：

| 方法名        | 详情          |
|:------------- |:-------------|
|GetName()|获取向量名称|
|SetName()|设置向量名称|
|Get(index)|访问向量的元素，返回Constant对象|
|CreateVector(dtype, size)|初始化一个指定大小的Vector，返回Vector对象|
|Append(Constant)|向Vector尾部追加一个对象|
|Remove(n)|移除末尾的n个元素|
|Get\<DataType>Slice|获取对应数据类型的切片|
|Set\<DataType>Array|将对应数据类型的切片赋值给向量|

具体示例如下：

* GetName、SetName

通过`GetName`获取向量名称，通过`SetName`设置向量名称。

```GO
p1.SetName("v1");
if  p1.GetName()!= "v1"  { t.Error("SetName Error"); }
```

* Get

使用`Get`方法获取向量某个下标的元素，从0开始，获取的也是Constant对象

```GO
p2 := p1.Get(0)；
if p2.GetInt()!= 1 { t.Error("Append Error"); }
```

* CreateVector

使用`CreateVector`函数创建一个空的Vector，这会返回一个Vector对象,参数type为DolphinDB的数据类型，size为向量的初始大小。

```GO
p1 := ddb.CreateVector(ddb.DT_INT,5);
```

* Append

对Vector调用`Append`方法可以向Vector尾部push一个对象，这有点类似于C++ vector的push_back方法

```GO
p1.Append(ddb.CreateInt(1));
```

* Remove

对Vector调用`Remove`，移除末尾的n个元素

```GO
p1.Remove(2);
```

* Get\<DataType>Slice

对Vector调用`Get<DataType>Slice`，获取该Vector对应类型的slice，类似于转换数据类型的方法。

```GO
p := conn.Run("5 4 8");
p1 := p.ToVector();
s1 := p1.GetIntSlice();
s2 := p1.GetShortSlice();
s3 := p1.GetLongSlice();
s4 := p1.GetFloatSlice();
s5 := p1.GetDoubleSlice();
s6 := p1.GetStringSlice();
```

查看p1.GetIntSlice()的结果，结果是一个Int类型的slice。
>[5 4 8]

* Set\<DataType>Array

对Vector调用`Set<DataType>Array`，将对应数据类型的切片赋值给对应类型的向量向量，例如：

```GO
rowNum := 10;
v1 := ddb.CreateVector(ddb.DT_BOOL,rowNum);
v2 := ddb.CreateVector(ddb.DT_INT, rowNum);
v3 := ddb.CreateVector(ddb.DT_FLOAT, rowNum);
v4 := ddb.CreateVector(ddb.DT_STRING, rowNum);
var arr1 []bool;
var arr2 []int32;
var arr3 []float32;
var arr4 []string;
for i := 0; i<rowNum; i++{
  arr1 = append(arr1, true);
  arr2 = append(arr2, 1);
  arr3 = append(arr3, 1.0);
  arr4 = append(arr4, "1");
}
v1.SetBoolArray(0,rowNum,arr1);
v2.SetIntArray(0,rowNum,arr2);  
v3.SetFloatArray(0,rowNum,arr3);  
v4.SetStringArray(0,rowNum,arr4);
```

查看v1.GetString()的结果，结果是一个Int类型的slice。
>[1,1,1,1,1,1,1,1,1,1]

需要注意的是，若要对16字节的字符串类型（包括DT_UUID，DT_IP和DT_INT128）向量赋值，需要以16位为单位进行赋值，即，数组的长度必须是16的倍数。下面的例子中，分别创建了长度为10的DT_UUID，DT_IP和DT_INT128类型的向量，并使用数组arr对这三个向量进行赋值，数组arr的长度为10*16。

```Go
rowNum := 10;
vuuid := ddb.CreateVector(ddb.DT_UUID, rowNum);
vipaddr := ddb.CreateVector(ddb.DT_IP, rowNum);
vint128 := ddb.CreateVector(ddb.DT_INT128, rowNum);
var arr []byte;
for i := 0; i<rowNum; i++{
  arr = append(arr,1,2,3,4,5,6,7,8,8,7,6,5,4,3,2,1);
}
vuuid.SetBinaryArray(0,rowNum,arr);
vipaddr.SetBinaryArray(0,rowNum,arr);
vint128.SetBinaryArray(0,rowNum,arr);
```

#### 5.3 Table类

Table类提供的较为常用的方法如下：

| 方法名        | 详情          |
|:------------- |:-------------|
|GetName()|获取表名称|
|SetName()|设置表名称|
|Columns()|获取列数|
|Rows()|获取行数|
|GetColumn(index)|访问表的下标为index的列，返回Vector对象|
|GetColumnName(index)|获取下标为index列的列名|
|GetColumnType(index)|获取下标为index列的数据类型|
|GetColumnbyName(name)|通过列名获取列，返回Vector对象|
|CreateTable(colname, coltype, size, capacity)|用列名和列创建一个Table，并指定初始大小和容量，返回Table对象|
|CreateTableByVector(colname,cols)|用列名和列创建一个Table，返回Table对象|


* GetColumn

使用`GetColumn`方法获取表某个下标的列，下标从0开始，返回一个Vector对象

```GO
t1.GetColumn(0);
```

* GetColumnName

使用`GetColumnName`方法获取表某个下标的列，下标从0开始，返回一个字符串

```GO
t1.GetColumnName(0);
```

* GetColumnType

使用`GetColumnType`方法获取表中指定列的类型，返回一个DolphinDB数据类型，各数据类型请参考附录。

```GO
t1.GetColumnType(0);
```

* GetColumnbyName

使用`GetColumnbyName`方法通过列名获取表中的某列，返回一个Vector对象

```GO
t1.GetColumnbyName("v1");
```

* CreateTable

下面的例子中，使用`CreateTableByVector`用列名和列创建一个Table，并指定初始大小和容量，返回Table对象，再对Table对象的每一列进行赋值。

```GO
colnames := [] string {"id","value"};
coltypes := [] int {ddb.DT_INT, ddb.DT_DOUBLE};
rowNum := 3;
const colNum = 2;
indexCapacity := 3;
ta := ddb.CreateTable(colnames, coltypes, rowNum, indexCapacity);
var columnVecs [colNum]ddb.Vector;
for i := 0; i<colNum; i++{
  columnVecs[i] = ta.GetColumn(i);
}
a1 := [] int32 {1,2,3};
a2 := [] float64 {1.5,2.7,3.9};
columnVecs[0].SetIntArray(0,3,a1);
columnVecs[1].SetDoubleArray(0,3,a2);
fmt.Println(ta.GetString());
```

* CreateTableByVector

使用`CreateTableByVector`方法通过列名和列创建一个Table，返回Table对象。需要注意的是，这里的列可以是已经有数值的列。

```GO
v1 := ddb.CreateVector(ddb.DT_INT,3);
v2 := ddb.CreateVector(ddb.DT_DOUBLE,3);
a1 := [] int32 {1,2,3};
a2 := [] float64 {1.5,2.7,3.9};
v1.SetIntArray(0,3,a1);
v2.SetDoubleArray(0,3,a2);
cols := [] ddb.Vector {v1,v2};
colnames := [] string {"id","value"};
ta := ddb.CreateTableByVector(colnames, cols);
fmt.Println(ta.GetString());
```

### 6. 上传数据对象

调用`Upload`方法，可以将一个Constant对象上传到DolphinDB数据库，对于非Constant类型，可以调用`ToConstant`方法将其转换为Constant类型对象。

```GO
p := conn.Run("5 4 8");
p1 := p.ToVector();
p2 := p1.ToConstant();
conn.Upload("vector1",p2);
```

### 7. 读写DolphinDB数据表

使用GO API的一个重要场景是，用户从其他数据库系统或是第三方Web API中取得数据后存入DolphinDB数据库中。本节将介绍通过GO API将取到的数据上传并保存到DolphinDB的数据表中。

DolphinDB数据表按存储方式分为三种:

* 内存表: 数据仅保存在内存中，存取速度最快，但是节点关闭后数据就不存在了。
* 本地磁盘表：数据保存在本地磁盘上。可以从磁盘加载到内存。
* 分布式表：数据分布在不同的节点，通过DolphinDB的分布式计算引擎，仍然可以像本地表一样做统一查询。

下面子分别介绍向三种形式的表中追加数据的实例。

首先，我们定义一个`CreateDemoTable`函数，该函数在GO环境中创建一个表，该具备3个列，类型分别是DT_STRING, DT_DATE和DT_DOUBLE，列名分别为name, date和price，并向该表中插入10条数据。

```GO
import (
   "strconv"
)

func CreateDemoTable() ddb.Table{
  colnames := [] string {"name","date","price"};
  coltypes := [] int {ddb.DT_STRING,ddb.DT_DATE,ddb.DT_DOUBLE};
  rowNum := 10;
  const colNum = 3;
  indexCapacity := 11;
  ta := ddb.CreateTable(colnames, coltypes, rowNum, indexCapacity);
  var columnVecs [colNum]ddb.Vector;
  for i := 0; i<colNum; i++{
    columnVecs[i] = ta.GetColumn(i);
  }
  var arr1 []string;
  var arr2 []int32;
  var arr3 []float64;
  for i := 0; i<rowNum; i++{
	arr1 = append(arr1, strconv.Itoa(i));
	arr2 = append(arr2, 17897);
	arr3 = append(arr3, 2.6);
  }
  columnVecs[0].SetStringArray(0,rowNum,arr1);
  columnVecs[1].SetIntArray(0,rowNum,arr2);
  columnVecs[2].SetDoubleArray(0,rowNum,arr3);
  fmt.Println(ta.GetString());
  return ta;
}
```

### 7.1 保存数据到DolphinDB内存表

在DolphinDB中，我们通过[table](http://www.dolphindb.cn/cn/help/table.html)函数来创建一个相同结构的内存表，指定表的容量和初始大小、列名和数据类型。由于内存表是会话隔离的，所以普通内存表只有当前会话可见。为了让多个客户端可以同时访问t，我们使用[share](http://www.dolphindb.cn/cn/help/share1.html)在会话间共享内存表。

```DolphinDB
t = table(100:0, `name`date`price, [STRING,DATE,DOUBLE]);
share t as tglobal;在会话间共享内存表。

```DolphinDB
t = table(100:0, `name`date`price, [STRING,DATE,DOUBLE]);
share t as tglobal;
```

在GO应用程序中，创建一个表，并调用`ToConstant`方法将表对象转换为Constant类型对象，再通过`RunFunc`函数调用DolphinDB内置的[tableInsert](http://www.dolphindb.cn/cn/help/tableInsert.html)函数将demotb表内数据插入到表tglobal中。

```GO
ta := CreateDemoTable();
tb := ta.ToConstant();
args := [] ddb.Constant{tb};
conn.RunFunc("tableInsert{tglobal}", args);
result := conn.Run("select * from tglobal");
fmt.Println(result.GetString());
```

### 7.2 保存数据到本地磁盘表

本地磁盘表通用用于静态数据集的计算分析，既可以用于数据的输入，也可以作为计算的输出。它不支持事务，也不持支并发读写。

在DolphinDB中使用以下脚本创建一个本地磁盘表，使用[database](http://www.dolphindb.cn/cn/help/database1.html)函数创建数据库，调用[saveTable](http://www.dolphindb.cn/cn/help/saveTable.html)命令将内存表保存到磁盘中：

```DolphinDB
t = table(100:0, `name`date`price, [STRING,DATE,DOUBLE]); 
db=database("~/demoDB"); 
saveTable(db, t, `dt); 
share t as tDiskGlobal;
```

与7.1小节的方法类似，我们通过将表Upload到服务器之后再向磁盘表追加数据。需要注意的是，[tableInsert](http://www.dolphindb.cn/cn/help/tableInsert.html)函数只把数据追加到内存，如果要保存到磁盘上，必须再次执行[saveTable](http://www.dolphindb.cn/cn/help/saveTable.html)函数。

```GO
ta := CreateDemoTable();
tb := ta.ToConstant();
args := [] ddb.Constant{tb};
conn.RunFunc("tableInsert{tDiskGlobal}", args);
conn.Run("saveTable(database('/home/hj/dbtest/demoDB'),tDiskGlobal,`dt)");
result := conn.Run("select * from tDiskGlobal");
fmt.Println(result.GetString());
```

### 7.3 保存数据到分布式表

分布式表是DolphinDB推荐在生产环境下使用的数据存储方式，它支持快照级别的事务隔离，保证数据一致性。分布式表支持多副本机制，既提供了数据容错能力，又能作为数据访问的负载均衡。下面的例子通过GO API把数据保存至分布式表。

请注意只有启用enableDFS=1的集群环境才能使用分布式表。

在DolphinDB中使用以下脚本创建分布式表，脚本中[database](http://www.dolphindb.cn/cn/help/database1.html)函数用于创建数据库，对于分布式数据库，路径必须以 dfs 开头。[createPartitionedTable](http://www.dolphindb.cn/cn/help/createPartitionedTable.html)函数用于创建分区表。

```DolphinDB
login(`admin, `123456)
dbPath = "dfs://demoDB";
tableName = `demoTable
db = database(dbPath, VALUE, 2019.01.01..2019.01.30)
pt=db.createPartitionedTable(table(100:0, `name`date`price, [STRING,DATE,DOUBLE]), tableName, `date)
```

DolphinDB提供[loadTable](http://www.dolphindb.cn/cn/help/loadTable.html)函数来加载分布式表，通过[tableInsert](http://www.dolphindb.cn/cn/help/tableInsert.html)函数追加数据，具体的脚本示例如下：
```GO
ta := CreateDemoTable();
tb := ta.ToConstant();
args := [] ddb.Constant{tb};
conn.RunFunc("tableInsert{loadTable('dfs://demoDB', `demoTable)}", args);
```

通过`Run`函数查看表内数据：

```GO
result := conn.Run("select * from loadTable('dfs://demoDB', `demoTable)");
fmt.Println(result.GetString());
```
结果为：

```
name date       price
---- ---------- -----
a    2019.01.01 1    
b    2019.01.02 2    
c    2019.01.03 3    
d    2019.01.04 4    
e    2019.01.05 5    
```

关于追加数据到DolphinDB分区表的实例可以参考example目录下的[分布式表的数据写入例子](./example/RdWrDFSTable.go)和[分布式表的多线程并行写入例子](./example/DFSWritingWithMultiThread.go) 。

### 7.4 读取和使用数据表

在GO API中，数据表保存为Table对象。由于Table是列式存储，所以若要在GO API中读取行数据需要先取出需要的列，再取出行。

假设在DolphinDB中如下定义的表，并插入了一些数据在表中：
```DolphinDB
kt = keyedTable(`col_int, 2000:0, `col_int`col_short`col_long`col_float`col_double`col_bool`col_string,  [INT, SHORT, LONG, FLOAT, DOUBLE, BOOL, STRING]);
```
如下例子通过`run`函数查询表内数据，对返回值用`ToTable`方法将其转换为一个Table对象，然后用`GetColumnByName`或`GetColumn`得到列，再一行行打印数据。

下面我们调用自定义的函数`CreateDemoTable`创建一个表，并且访问表中的元素。

```GO
res := conn.Run("select top 3 * from kt")
resTable := res.ToTable();
col0 := resTable.GetColumnByName("col_int");
col1 := resTable.GetColumnByName("col_short");
col2 := resTable.GetColumnByName("col_long");
col3 := resTable.GetColumnByName("col_float");
col4 := resTable.GetColumn(4);
col5 := resTable.GetColumn(5);
col6 := resTable.GetColumn(6);
for i := 0;i < resTable.Rows(); i++ {
  col0i,col1i,col2i,col3i,col4i,col5i,col6i := col0.Get(i),col1.Get(i),col2.Get(i),col3.Get(i),col4.Get(i),col5.Get(i),col6.Get(i);
  fmt.Printf("%v,%v,%v,%v,%v,%v,%v\n",col0i.GetInt(),col1i.GetShort(),col2i.GetLong(),col3i.GetFloat(),
    col4i.GetDouble(), col5i.GetBool(), col6i.GetString())
}
```
输出结果为：
```Conosle
0,255,10000,133.3,255,true,str
1,255,10001,133.3,255,true,str
2,255,10002,133.3,255,true,str
```

附录
---
* [Go API 使用样例](example/README_CN.md)

* 数据形式列表（`GetForm`函数返回值对应的数据形式）

| 序号       | 数据形式          |
|:------------- |:-------------|
|0|DF_SCALAR
|1|DF_VECTOR
|2|DF_PAIR
|3|DF_MATRIX
|4|DF_SET
|5|DF_DICTIONARY
|6|DF_TABLE
|7|DF_CHART
|8|DF_CHUNK

数据类型列表（`GetType`函数返回值对应的数据类型）

| 序号       | 数据类型          |
|:------------- |:-------------|
|1|DT_BOOL
|2|DT_CHAR
|3|DT_SHORT
|4|DT_INT
|5|DT_LONG
|6|DT_DATE
|7|DT_MONTH
|8|DT_TIME
|9|DT_MINUTE
|10|DT_SECOND
|11|DT_DATETIME
|12|DT_TIMESTAMP
|13|DT_NANOTIME
|14|DT_NANOTIMESTAMP
|15|DT_FLOAT
|16|DT_DOUBLE
|17|DT_SYMBOL
|18|DT_STRING
|19|DT_UUID
|28|DT_DATEHOUR
|29|DT_DATEMINUTE
|30|DT_IP
|31|DT_INT128