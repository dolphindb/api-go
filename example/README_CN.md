# DolphinDB Go API 使用样例

### 1. 概述

#### 1.1 环境配置

- 安装Go语言并配置环境变量。在api-go/目录下使用如下指令添加环境变量。请注意，执行export指令只能临时添加环境变量，若需要让变量持久生效，请根据Linux相关教程修改系统文件。

```bash
$ cd api-go/
$ export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/api
```

- 搭建DolphinDB Server。 详见[DolphinDB 教程](https://github.com/dolphindb/Tutorials_CN/blob/master/single_machine_cluster_deploy.md)。

启动DolphiniDB Server后，请根据本地实际的DolphinDB配置修改样例代码中的节点地址、端口、用户名和密码。本教程使用默认地址“localhost”，默认端口8848，用户名“admin”， 密码“123456“。

#### 1.2 样例说明

目前有5个Go API的样例代码，如下表所示，均位于example目录下：

* [RdWrMemoryTable.go](./RdWrMemoryTable.go): 介绍了对内存表的写入和读取操作的方式。
* [RdWrDFSTable.go](./RdWrDFSTable.go): 分布式表的数据写入。
* [DFSWritingWithMultiThread.go](./DFSWritingWithMultiThread.go): 在集群部署下的多线程并行写入数据库。
* [StreamingDataWriting.go](./StreamingDataWriting.go): 流数据写入的样例。
* [StreamingThreadClientSubscriber.go](./StreamingThreadClientSubscriber.go): 流数据订阅的样例。

这些例子的开发环境详见[DolphinDB Go API](https://github.com/dolphindb/api-go/blob/master/README.md)。

在api-go/目录下通过go run命令执行go文件， 即可运行样例代码：

```bash
go run ./example/RdWrMemoryTable.go
```

#### 1.3 连接到DolphinDB Server

Go API需要连接到DolphinDB Server后才可以读写数据，这需要先声明一个DBConnection对象。

```Go
var conn ddb.DBConnection;
conn.Init();
conn.Connect(host,port,username,password); 
```

在声明对象后，调用`Init`方法初始化连接，并调用`Connect`方法建立一个到DolphinDB Server的连接。

下面对每个例子进行简单说明。

### 2. 内存表的读写

#### 2.1 创建内存表

在Go客户端中调用conn对象的`Run`方法，能够在DolphinDB Server端执行DolphinDB脚本。

```Go
script := "kt = keyedTable(`col_int, 2000:0, `col_int`col_short`col_long`col_float`col_double`col_bool`col_string,  [INT, SHORT, LONG, FLOAT, DOUBLE, BOOL, STRING]); "
conn.Run(script)
```

这段脚本会在Server端创建一个名为kt的带主键的内存表，这个表有7列，分别是7种基本数据类型INT， SHORT， LONG， FLOAT， DOUBLE， BOOL， STRING。

#### 2.2 写入数据

上面在Server端创建了一个内存表，下面在Go中创建7个列来写入数据。

1. 在Go中使用`CreateVector`函数创建对应类型的列。

```Go
coltypes := [] int {ddb.DT_INT, ddb.DT_SHORT, ddb.DT_LONG, ddb.DT_FLOAT, ddb.DT_DOUBLE, ddb.DT_BOOL, ddb.DT_STRING};
colnum := 7;
rownum := 20;
colv :=  [11]ddb.Vector{};
for i:=0;i<colnum;i++{
    colv[i] = ddb.CreateVector(coltypes[i], 0);
}
```

2. 准备数据。在Go中创建7个数组，对应表的7列，并且依次向数组添加数据。

```Go
v0 := [] int32{}
v1 := [] int16{}
v2 := [] int64{}
v3 := []  float32{}
v4 := [] float64{}
v5 := []  bool{}
v6 := [] string{}
for i:=0;i< rownum;i++{
  v0 = append(v0, int32(i))
	v1 = append(v1, 255)
	v2 = append(v2, 10000)
	v3 = append(v3, 133.3)
	v4 = append(v4, 255.0)
	v5 = append(v5, true)
	v6 = append(v6, "str")
}
```

3. 使用API提供的`Append`方法将数组追加到表示列的向量。

```Go
colv[0].AppendInt(v0,  rownum)
colv[1].AppendShort(v1,  rownum)
colv[2].AppendLong(v2,  rownum)
colv[3].AppendFloat(v3,  rownum)
colv[4].AppendDouble(v4,  rownum)
colv[5].AppendBool(v5,  rownum)
colv[6].AppendString(v6,  rownum)
```	 

4. 对连接对象conn调用`RunFunc`将各列的数据写入到kt中。其中args是一个Constant类型的GO slice。写入时使用了DolphinDB的[tableInsert](https://www.dolphindb.cn/cn/help/index.html?tableInsert.html)函数。

```Go
args := []ddb.Constant{colv[0].ToConstant(), colv[1].ToConstant(), colv[2].ToConstant(), 
                       colv[3].ToConstant(),colv[4].ToConstant(),colv[5].ToConstant(),colv[6].ToConstant()}
script2 := "tableInsert{kt}"
conn.RunFunc(script2,args)
```

`RunFunc`的第二个参数是一个Constant类型的slice，可以将多个Constant对象加入其中。其他的诸如Table和Vector类型，需要调用`ToConstant`方法转换成Constant类型的对象。

> 请注意: 这里通过脚本使用了DolphinDB的[`tableInsert`](https://www.dolphindb.cn/cn/help/tableInsert.html)函数，对于分区表，`tableInsert`的第二个参数不能如样例中是多个Vector组成的slice，而只能是一个表。

#### 2.3 读取数据

我们可以通过直接执行DolphinDB SQL查询语句，如select * from kt， 从Server端读取数据，如

```Go
res := conn.Run ("select * from kt")
```

这里我们可以确定返回的是一个表，对其调用`ToTable`方法将其转换为一个Table对象，就可以适用于Table类的各种方法，类的更多方法可以参考Go API的文档。

```Go
res_table := res.ToTable();
fmt.Println(res_table.Rows(), " rows ",)
fmt.Println(res_table.Columns(), " columns ")
```

对于res_table， 可以通过`GetColumn`的方式获取各列，Table的列是Vector类型，而后使用`Get`方法获取列中单个的值，或者使用`GetDataTypeSlice`方法返回整个个列的slice。

```
colres :=  [11]ddb.Vector{};
for i:=0;i<colnum;i++{
    colres[i] = res_table.GetColumn(i);
}
v1 := colres[0].Get(0)
s1 := colres.GetIntSlice()
```

Constant以及Table和Vector等类均有`GetString`方法以直观地打印数据。

```Go
re1 := conn.Run("select  top 5 * from kt")
fmt.Println(re1.GetString()) 
```

另外，通过`Run`方法返回的是一个Constant对象，当不确定它的数据形式时，需要通过调用`GetForm`方法来判断。

```Go
res := conn.Run ("select * from kt")
from_number := res.GetForm()
```

GetFrom方法返回数据形式对应的数字，具体的对应规则请参考[Go API README](https://github.com/dolphindb/api-go/blob/master/README.md#%E9%99%84%E5%BD%95)。

### 3. 分布式表的读写

本例实现了用单线程往分布式数据库写入数据的功能。

#### 3.1 创建分布式表和数据库

在DolphinDB中执行以下脚本创建分布式表和数据库:

```
t = table(100:0, `id`date`x , [INT, DATE, DOUBLE]); share t as tglobal;
login(`admin, `123456); 
dbPath='dfs://datedb'; 
if(existsDatabase(dbPath)) dropDatabase(dbPath); 
db=database(dbPath, VALUE, 2017.08.07..2017.08.11); 
tb=db.createPartitionedTable(t, `pt,`date)
```

该分布式表采用了根据日期的值分区。

#### 3.2 数据写入

在写入数据到分布式表中时，用于分区的列中的值需要符合分区方式，才可以正常写入到分布式表中。例如写入样例中的这个分布式表，需要本地的表中date这一列是DolphinDB的DATE类型，且值介于2017.08.07..2017.08.11之间，否则会无法写入数据。

1. 准备数据。

```Go
rownum := 1000;
v1 := ddb.CreateVector(ddb.DT_INT, 0)
v2 := ddb.CreateVector(ddb.DT_DATE, 0)
v3 := ddb.CreateVector(ddb.DT_DOUBLE, 0)
for i:=0;i<rownum;i++{
  v1.Append(ddb.CreateInt(i));
  v2.Append(ddb.CreateDate(2017,8 ,7+ i%5));
  v3.Append(ddb.CreateDouble(3.1415926));
}

cols := [] ddb.Vector {v1,v2,v3,};
colnames := [] string {"id","date","x"};
t := ddb.CreateTableByVector(colnames, cols);
```

2. 向DFS表写入数据。DolphinDB的分布式表需要使用[loadTable](https://www.dolphindb.cn/cn/help/index.html?loadTable.html)加载获取后才能进行修改操作。另外由于目标表是分布式表，参数args不能是内存表样例中的Vector slice， 而必须是一个表。

```Go
args := [] ddb.Constant{t.ToConstant()};
conn.RunFunc("tableInsert{loadTable('dfs://datedb', `pt)}", args);
```

#### 3.3 从分布式数据库中读取数据

和上一节相同，执行SQL语句同样需要使用`loadTable`加载数据库中的表。

```
res := conn.Run("select count(*) from loadTable('dfs://datedb', `pt)")
fmt.Println(res.GetString())
```

> 请注意: 在分区数量多且数据庞大时，读取操作会较为缓慢。

单线程写入会有瓶颈，多线程和批量写入能显著提高DolphinDB的吞吐量和写入性能，建议在实际环境中采用多线程并批量写入数据。

### 4. 多线程并行写入数据库

在执行本例前需要先将main函数中hosts与ports修改为DolphinDB Server节点列表，并在dfs上创建表。

#### 4.1 创建分布式数据库和表

在本例中写入数据规模非常大，场景要求单机每秒300万条写入。故分布式数据库第一层按时间分区，第二层是按IP地址分50个HASH分区。第三层仍然是按IP地址分50个HASH分区。在实际写入时采用10个线程来写入。

在DolphinDB中执行以下脚本创建数据库和表。

```
dbName = "dfs://natlog"
tableName = "natlogrecords"
db1 = database("", VALUE, datehour(2019.09.11T00:00:00)..datehour(2019.12.30T00:00:00) )
db2 = database("", HASH, [IPADDR, 50]) 
db3 = database("", HASH,  [IPADDR, 50]) 
db = database(dbName, COMPO, [db1,db2,db3])
data = table(1:0, ["fwname","filename","source_address","source_port","destination_address","destination_port","nat_source_address","nat_source_port","starttime","stoptime","elapsed_time"], [SYMBOL,STRING,IPADDR,INT,IPADDR,INT,IPADDR,INT,DATETIME,DATETIME,INT])
db.createPartitionedTable(data,tableName,`starttime`source_address`destination_address)
```

#### 4.2 多线程写入数据

在写入数据时要注意的是，并行写入时，多个线程不能同时往DolphinDB分布式数据库的同一个分区写数据，所以产生数据时，要保证每个线程写入的数据是属于不同分区的。

本例通过为每个写入线程平均分配分区的方法（比如10个线程，50个分区，则线程1写入1-5，线程2写入6-10，其他线程依次类推），保证多个写入线程写到不同的分区。其中每个IP地址的hash值是通过API内置的`GetHash`计算的：得到相同的hash值，说明数据属于相同分区，反之属于不同分区。

注意：若分布式数据库不是HASH分区，可以通过如下方式确保不同的线程写不同的分区：

* 若采用了范围（RANGE）分区，可以先在server端执行函数schema(database(dbName)).partitionSchema[1]获取到分区字段的分区边界（partitionSchema取第一个元素的前提是一般数据库采用两层分区，第一层是日期，第二层是设备或股票进行范围分区）。然后对比数据的分区字段的取值和分区的边界值，控制不同的线程负责不同的1个或多个分区。

* 对于分区类型为值（VALUE）分区、列表（LIST）分区，用值比较的方法可以判定数据所属的分区。然后不同的线程负责写1个或多个不同分区。

例如，本例`createDemoTable`函数中的这一段代码通过`GetHash`方法，对buckets取余获取适合分区的IP值。

```Go
spIP := ddb.CreateConstant(ddb.DT_IP);
for j := 1; j<255; j++ {
  sip[0] = byte(j);
  spIP.SetBinary(sip);
  x := byte(spIP.GetHash(50));
  if (x >= startp && x<startp + pcount){
    break;}
}
```

多线程写入的具体流程是：用go装载函数开启多线程，将要写入的数据准备好，然后对每个节点都获得一次连接，以多节点同时写入。写入时使用`runtime.Gosched`让出CPU时间片。以实现多线程。CreateDemoTable函数可以参考样例代码。

多线程写入示例:

```GO
func finsert(rows int, startp byte, pcount byte,starttime int , timeInc int, hosts []string, ports []int, p int, inserttimes int){
  var conn ddb.DBConnection;
  conn.Init();
  success := conn.Connect(hosts[p], ports[p], username, password)   // 线程连接到对应的节点hosts[p], ports[p]
  if (!success) {panic("connect failed!");};  
  t := CreateDemoTable(rows, startp, pcount, starttime, timeInc);
  tb := t.ToConstant();
  args := [] ddb.Constant{tb};
  for i:=0;i<inserttimes;i++{                               // 写入inserttimes次
    conn.RunFunc("tableInsert{loadTable('dfs://natlog', `natlogrecords)}",args);
    runtime.Gosched();
  }
}

func main(){
  runtime.GOMAXPROCS(10)
  hosts := []string{"192.168.1.12","192.168.1.13","192.168.1.14","192.168.1.15","192.168.1.12","192.168.1.13","192.168.1.14","192.168.1.15", "192.168.1.12","192.168.1.13"}
  ports :=  []int{19162,19162,19162,19162,19163,19163,19163,19163,19164,19164}
  lh := len(hosts);
  if lh != len(ports) {panic("Hosts and ports should have equal length !");}
  if lh >10 {panic("Hosts should be fewer than  10 !");}
  c := make(chan int, lh);
  tablerows := 10000;
  inserttimes := 100;
  for i:=0;i<lh;i++{
    go finsert(tablerows, byte(i*5-1), byte(5), int(ddb.GetEpochTime()/1000), i*5, hosts, ports, i, inserttimes);
  }
}
```

示例中提供了多个hosts和ports，也即多个DolphinDB Server节点，以供多个线程分别连接并写入。通过多个节点写入可以负载均衡，提高写入效率。

### 5. 流数据写入和订阅

使用流数据需要配置发布节点和订阅节点，详见
[DolphinDB 流数据教程](https://github.com/dolphindb/Tutorials_CN/blob/master/streaming_tutorial.md)。

#### 5.1 流数据写入

##### 5.1.1 创建流表

首先在DolphinDB Server端的流数据发布节点上执行以下脚本创建流表：

```DolpinDB
st=streamTable(1000000:0,`id`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cuuid`cip`cint128,[LONG,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,IPADDR,INT128])
enableTableShareAndPersistence(st,"st1",true,false,1000000)
```

##### 5.1.2 写数据

在DolphinDB Server端创建流表后，在Go中写入数据到流表。写入流程与写入内存表基本相同，创建要写入的表，连接节点，写入数据。

因为该流表是共享表，所以写入时，在tableInsert中使用[objByName](https://www.dolphindb.cn/cn/help/index.html?objByName.html)，即可获取到该流表。

```Go
conn.RunFunc("tableInsert{objByName(`st1)}",args);
```

#### 5.2 流数据订阅

API提供PollingClient类型订阅流表的数据。PollingClient返回一个消息队列，用户可以通过轮查询的方式获取和处理数据。

##### 5.2.1 创建client对象

定义一个PollingClient对象，随机产生一个端口号用于监听。

```Go
var client ddb.PollingClient;
listenport  := rand.Intn(1000)+50000;
client.New(listenport);
```

##### 5.2.2 订阅数据并处理

然后调用`Subscribe`方法，注意其中的host参数不能是localhost。

```Go
queue := client.Subscribe(host, port, "st1",  ddb.Def_action_name(), 0);
```

该方法返回一个消息队列。

不断对获得的消息队列调用Poll即可获得数据流。通过`IsNull`方法来判断是否读取完毕。

```Go
msg := ddb.CreateConstant(ddb.DT_VOID)
for true {
    if (queue.Poll(msg, 1000)) {
        if msg.IsNull() {
          break;
        }
    fmt.Println("Get message at",time.Now().String())
    }
}
```

##### 5.2.3 取消订阅

对client调用`UnSubscribe`方法，取消订阅流数据。

```
client.UnSubscribe(host, port, "st1",  ddb.Def_action_name())
```