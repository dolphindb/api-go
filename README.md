# DolphinDB Go API 概述

DolphinDB Go API 目前仅支持Linux开发环境。

本教程主要介绍以下内容：

- 项目编译
- 建立DolphinDB连接
- 运行DolphinDB脚本
- 运行函数
- 数据对象介绍
- 上传本地对象到DolphinDB服务器
- 追加数据到DolphinDB数据表

### 1.导API包
可参考api-go目录下example.go文件,包名简写为ddb

```GO
package main
import (
	 "./api"
)
func main() {
  var conn ddb.DBConnection;
  conn.Init();
  conn.Connect("localhost",1621,"admin","123456");
  
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

如下脚本声明了一个`DBConnection`对象，并调用`Init`方法初始化对象。请注意，GO API 在定义DBConnection对象之后必须首先调用`Init`方法来进行初始化配置，否则可能会导致不能正常使用。

```GO
var conn DBConnection;
conn.Init();
```

GO API通过TCP/IP协议连接到DolphinDB。使用`connect`方法创建连接时，需要提供DolphinDB Server的IP、端口号、用户名及密码，函数返回一个布尔值表示是否连接成功。

```GO
conn.Connect("localhost",8848,"admin","123456");
```

### 3. 运行DolphinDB脚本

通过`Run(script)`方法运行DolphinDB脚本：

```GO
v := conn.Run("`IBM`GOOG`YHOO");
fmt.Println(v.GetString());
```
输出结果为：
>["IBM","GOOG","YHOO"]

### 4. 运行函数

通过`RunFunc(script,args)`方法运行DolphinDB脚本的方式调用DolphinDB的内置函数：

```GO
v1 := CreateVector(DT_INT);
v2 := CreateVector(DT_INT)
v1.Append(CreateInt(1));
v2.Append(CreateInt(5));
x := v1.ToConstant();
y := v2.ToConstant();
args := [] Constant{x, y};
result1 := conn.RunFunc("add", args);
fmt.Println(result1.GetString());
```

输出结果为：
>[6]

### 5. 数据对象介绍

DolphinDB GO API 通过Constant这一基本类型接受各种类型的数据，包括int、float等。同时，GO API还提供Vector类和Table类来存放向量和表对象。

#### 5.1 Constant类

Constant类提供的较为常用的方法如下：

| 方法名        | 详情          |
|:------------- |:-------------|
|GetForm()|获取对象类型|
|GetType()|获取数据类型|
|Size()|获取对象大小|
|`<GetDataType>`|将Constant对象转换为GO中的基本数据类型|
|`<IsDataForm>`|校验Constant对象存放的数据类型|
|ToVector()|转换为Vector类型|

具体示例如下：

* `GetForm()`、`GetType()`

对Constant对象调用`GetForm`方法获取对象类型，调用`GetType`方法获取数据类型。需要注意的是，这两个方法返回的不是字符串，而是对象类型或者数据类型对应的序号，具体对应关系见附录。

```GO
x := conn.Run("1+1");
x.GetForm();
x.GetType();
```
输出结果为如下，其中，form=0代表是form为scalar，type=4代表type为int。

>0 4

* `Size()`

Size方法可以获取对象大小，对于Vector会获取长度，对于Table会获取行数

```GO
p.Size();
```

* `<GetDataType>`

通过`<GetDataType>`系列方法，将Constant对象转换为GO语言中的常用类型

```GO
x := conn.Run("1+1");
x.GetInt();  //转换为整形int
x.GetLong();  //int64
x.GetShort();  //int16
X.GetBool(); //转换为布尔型
x.GetString()  //转换为字符串
x.GetFloat(); //float32
x.GetDouble();  //float64
```

* `<IsDataForm>`

使用`IisDataForm>`系列方法，校验对象存放的数据的类型

```GO
x := conn.Run("2 3 5");
x.IsScalar();
x.IsVector();
x.IsTable();
```

对Constant对象调用ToVector可以获得一个Vector对象，Vector类的介绍见5.2小节。

```GO
p := conn.Run("5 4 8");
p1 := p.ToVector();
```

类似地，对Constant对象调用`ToTable`可以获得一个Table对象。

#### 5.2 Vector类

Vector(向量)是DolphinDB中常用的类型，也可作为表中的一列,Vector类提供的较为常用的方法如下：

| 方法名        | 详情          |
|:------------- |:-------------|
|GetName()|获取向量名称|
|SetName()|设置向量名称|
|Get(index)|访问向量的元素，返回Constant对象|
|CreateVector(type)|创建一个空的Vector，返回Vector对象|
|Append(Constant)|向Vector尾部追加一个对象|
|Remove(n)|移除末尾的n个元素|
|`<GetDataTypeSlice>`|    获取对应数据类型的切片  |

具体示例如下：

* `GetName()`、`SetName()`

通过`GetName()`获取向量名称，通过`SetName()`设置向量名称。

```GO
p1.SetName("v1");
if  p1.GetName()!= "v1"  { t.Error("SetName Error"); }
```

* `Get(index)`

使用`Get(index)`方法获取向量某个下标的元素，从0开始，获取的也是Constant对象

```GO
p2 := p1.Get(0)；
if p2.GetInt()!= 1 { t.Error("Append Error"); }
```

* `CreateVector(type)`

使用`CreateVector(type)`函数创建一个空的Vector，这会返回一个Vector对象,参数type为DolphinDB的数据类型

```GO
p1 := CreateVector(DT_INT);
```
* `Append(Constant)`

对Vector调用`Append(Constant)`方法可以向Vector尾部push一个对象，这有点类似于C++ vector的push_back方法

```GO
p1.Append(CreateInt(1));
```

* `Remove(n)`

对Vector调用`Remove(n)`，移除末尾的n个元素

```GO
p1.Remove(2);
```

* `<GetDataTypeSlice>`

对Vector调用`<GetDataTypeSlice>`，获取该Vector对应类型的slice，类似于转换数据类型的方法。

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

查看p1.GetIntSlice()的结果，结果是一个Int类型的向量。
>[5 4 8]

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
|CreateTable(colname,cols)|用列名和列创建一个Table，返回Table对象|


* `GetColumn(index)`

使用`GetColumn(index)`方法获取表某个下标的列，从0开始，返回一个Vector对象

```GO
t1.GetColumn(0);
```

* `GetColumnName(index)`

使用`GetColumnName(index)`方法获取表某个下标的列，从0开始，返回一个字符串

```GO
t1.GetColumnName(0);
```

* `GetColumnType(index)`

使用`GetColumnType(index)`方法获取表某个下标的列类型，返回一个DolphinDB数据类型，参考附录

```GO
t1.GetColumnType(0);
```

* `GetColumnbyName(name)`

使用`GetColumnbyName(name)`方法通过列名获取表中的某列，返回一个Vector对象

```GO
t1.GetColumnbyName("v1");
```

* `CreateTable(colname,cols)`

使用`CreateTable(colname,cols)`方法通过列名和列的数据类型创建一个Table，返回Table对象

```GO
v1 := CreateVector(DT_INT);
v2 := CreateVector(DT_INT)
cols := [] Vector {v1,v2};
v1.Append(CreateInt(1));
v2.Append(CreateInt(1));
colnames := [] string {"v1","v2"};
ta := CreateTable(colnames, cols);
fmt.Println(ta.GetString());
```

### 6. 上传数据对象

调用`Upload`方法，可以将一个Constant对象上传到DolphinDB数据库，对于非Constant类型，可以调用`ToConstant`方法将其转换为Constant类型对象.

```GO
p := conn.Run("5 4 8");
p1 := p.ToVector();
p2 := p1.ToConstant();
conn.Upload("vector1",p2);
```

### 7. 追加数据到DolphinDB数据表

使用GO API的一个重要场景是，用户从其他数据库系统或是第三方Web API中取得数据后存入DolphinDB数据库中。本节将介绍通过GO API将取到的数据上传并保存到DolphinDB的数据表中。

DolphinDB数据表按存储方式分为三种:

* 内存表: 数据仅保存在内存中，存取速度最快，但是节点关闭后数据就不存在了。
* 本地磁盘表：数据保存在本地磁盘上。可以从磁盘加载到内存。
* 分布式表：数据分布在不同的节点，通过DolphinDB的分布式计算引擎，仍然可以像本地表一样做统一查询。

下面子分别介绍向三种形式的表中追加数据的实例。

首先，我们定义一个`CreateDemoTable`函数，该函数在GO环境中创建一个表，该具备3个列，分别是DT_STRING, DT_DATE, DT_DOUBLE类型，列名分别为name, date和price，并向该表中插入10条数据。

```GO
func CreateDemoTable() Table{
	rowNum :=10;
	v1 := CreateVector(DT_STRING);
	v2 := CreateVector(DT_DATE);
	v3 := CreateVector(DT_DOUBLE);
	for i :=0; i<rowNum; i++{
		v1.Append(CreateString("1"));
		v2.Append(CreateDate(2019, 1, 1));
		v3.Append(CreateDouble(1.0));
	}
	cols := [] Vector {v1,v2,v3};
	colnames := [] string {"name","date","price"};
	return CreateTable(colnames, cols);
}
```

### 7.1 保存数据到DolphinDB内存表

在DolphinDB中，我们通过`table`函数来创建一个相同结构的内存表，指定表的容量和初始大小、列名和数据类型。由于内存表是会话隔离的，所以普通内存表只有当前会话可见。为了让多个客户端可以同时访问t，我们使用`share`在会话间共享内存表。

```
t = table(100:0, `name`date`price, [STRING,DATE,DOUBLE]);
share t as tglobal;
```

在GO应用程序中，创建一个表，并调用`ToConstant()`方法将表对象转换为Constant类型对象，再通过`RunFunc`函数调用DolphinDB内置的`TableInsert`函数将demotb表内数据插入到表tglobal中。

```GO
ta := CreateDemoTable();
tb := ta.ToConstant();
args := [] Constant{tb};
conn.RunFunc("tableInsert{tglobal}", args);
result :=conn.Run("select * from tglobal");
fmt.Println(result.GetString());
```

### 7.2 保存数据到本地磁盘表

本地磁盘表通用用于静态数据集的计算分析，既可以用于数据的输入，也可以作为计算的输出。它不支持事务，也不持支并发读写。

在DolphinDB中使用以下脚本创建一个本地磁盘表，使用`database`函数创建数据库，调用`saveTable`函数将内存表保存到磁盘中：

```
t = table(100:0, `name`date`price, [STRING,DATE,DOUBLE]); 
db=database("~/demoDB"); 
saveTable(db, t, `dt); 
share t as tDiskGlobal;
```

与6.1小节的方法类似，我们通过将表Upload到服务器之后再向磁盘表追加数据。需要注意的是，`tableInsert`函数只把数据追加到内存，如果要保存到磁盘上，必须再次执行`saveTable`函数。

```GO
ta := CreateDemoTable();
tb := ta.ToConstant();
args := [] Constant{tb};
conn.RunFunc("tableInsert{tDiskGlobal}", args);
conn.Run("saveTable(database('/home/hj/dbtest/demoDB'),tDiskGlobal,`dt)");
result :=conn.Run("select * from tDiskGlobal");
fmt.Println(result.GetString());
```

### 7.3 保存数据到分布式表

分布式表是DolphinDB推荐在生产环境下使用的数据存储方式，它支持快照级别的事务隔离，保证数据一致性。分布式表支持多副本机制，既提供了数据容错能力，又能作为数据访问的负载均衡。下面的例子通过GO API把数据保存至分布式表。

请注意只有启用enableDFS=1的集群环境才能使用分布式表。

在DolphinDB中使用以下脚本创建分布式表，脚本中，`database`函数用于创建数据库，对于分布式数据库，路径必须以“dfs”开头。`createPartitionedTable`函数用于创建分区表。

```DolphinDB
login(`admin, `123456)
dbPath = "dfs://demoDB";
tableName = `demoTable
db = database(dbPath, VALUE, 2019.01.01..2019.01.30)
pt=db.createPartitionedTable(table(100:0, `name`date`price, [STRING,DATE,DOUBLE]), tableName, `date)
```

DolphinDB提供`loadTable`F方法来加载分布式表，通过`tableInsert`方式追加数据，具体的脚本示例如下：

```GO
ta := CreateDemoTable();
tb := ta.ToConstant();
args := [] Constant{tb};
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
1    2019.01.01 1    
1    2019.01.01 1    
1    2019.01.01 1    
1    2019.01.01 1    
1    2019.01.01 1    
```

附录：

---
数据形式列表

| 序号       | 数据形式          |
|:------------- |:-------------|
|0|DF_VECTOR
|1|DF_PAIR
|2|DF_MATRIX
|3|DF_SET
|4|DF_DICTIONARY
|5|DF_TABLE
|6|DF_CHART
|7|DF_CHUNK

数据类型列表

| 序号       | 数据类型          |
|:------------- |:-------------|
|0|DT_BOOL
|1|DT_CHAR
|2|DT_SHORT
|3|DT_INT
|4|DT_LONG
|5|DT_DATE
|6|DT_MONTH
|7|DT_TIME
|8|DT_MINUTE
|9|DT_SECOND
|10|DT_DATETIME
|11|DT_TIMESTAMP
|12|DT_NANOTIME
|13|DT_NANOTIMESTAMP
|14|DT_FLOAT
|15|DT_DOUBLE
|16|DT_SYMBOL
|17|DT_STRING
|18|DT_UUID
|19|DT_FUNCTIONDEF
|20|DT_HANDLE
|21|DT_CODE
|22|DT_DATASOURCE
|23|DT_RESOURCE
|24|DT_ANY
|25|DT_COMPRESS
|26|DT_DICTIONARY
|27|DT_OBJECT