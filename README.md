<h1 align="center">DolphinDB Go API</h1>

[![GitHub release](https://img.shields.io/github/release/dolphindb/api-go.svg?style=flat-square)](https://github.com/dolphindb/api-go/releases/latest)
[![PkgGoDev](https://img.shields.io/badge/go.dev-docs-007d9c?style=flat-square&logo=go&logoColor=white)](https://pkg.go.dev/github.com/dolphindb/api-go)

欢迎使用 DolphinDB Go API。您可以使用 Go API 连接 DolphinDB 数据库，进行建库建表，以及读取、写入数据等操作。

- [1. Go API 概述](#1-go-api-概述)
- [2. 安装依赖](#2-安装依赖)
- [3. DolphinDB 使用示例](#3-dolphindb-使用示例)
  - [3.1. 初始化 DolphinDB](#31-初始化-dolphindb)
  - [3.2. 通过 API 建库建表](#32-通过-api-建库建表)
  - [3.3. 基础函数使用](#33-基础函数使用)
    - [3.3.1. 构造数据类型](#331-构造数据类型)
      - [3.3.1.1. NewDataType 入参对照表](#3311-newdatatype-入参对照表)
      - [3.3.1.2. NewDataTypeListFromRawData 入参对照表](#3312-newdatatypelistfromrawdata-入参对照表)
      - [3.3.1.3. Null 值对照表](#3313-null-值对照表)
    - [3.3.2. 完整示例](#332-完整示例)
  - [3.4. 初始化 DBConnectionPool](#34-初始化-dbconnectionpool)
- [4. 读写 DolphinDB 数据表](#4-读写-dolphindb-数据表)
  - [4.1. 读取和使用数据表](#41-读取和使用数据表)
    - [4.1.1. 读取数据表](#411-读取数据表)
    - [4.1.2. 使用 Table 对象](#412-使用-table-对象)
  - [4.2. 保存数据到 DolphinDB 数据表](#42-保存数据到-dolphindb-数据表)
    - [4.2.1. 同步追加数据](#421-同步追加数据)
      - [4.2.1.1. 使用 `insert into` 追加单行数据](#4211-使用-insert-into-追加单行数据)
      - [4.2.1.2. 使用 `tableInsert` 函数向表中批量追加数组对象](#4212-使用-tableinsert-函数向表中批量追加数组对象)
      - [4.2.1.3. 使用 `tableInsert` 函数向表中追加 `Table` 对象](#4213-使用-tableinsert-函数向表中追加-table-对象)
    - [4.2.2. 异步追加数据](#422-异步追加数据)
      - [4.2.2.1. 数据表的并发写入](#4221-数据表的并发写入)
  - [4.3. 批量异步追加数据](#43-批量异步追加数据)
    - [4.3.1. MultiGoroutineTable](#431-multigoroutinetable)
    - [4.3.2. MultiGoroutineTable 常见错误](#432-multigoroutinetable-常见错误)
- [5. 流数据 API](#5-流数据-api)
  - [5.1. 代码示例:](#51-代码示例)
  - [5.2. 断线重连](#52-断线重连)
  - [5.3. 启用 Filter](#53-启用-filter)
  - [5.4. 取消订阅](#54-取消订阅)
- [6. 工具方法](#6-工具方法)
  - [6.1. model 包](#61-model-包)

## 1. Go API 概述

Go API 需要运行在 golang 1.15 或以上版本的环境。
Go API定义了 DataForm 接口，表示服务器端返回的[数据形式](https://www.dolphindb.cn/cn/help/130/DataTypesandStructures/DataForms/index.html)。该接口提供 `GetDataForm` 方法获取数据形式的整型表示。目前支持获取 7 种数据形式的整型表示。可以根据得到的整型表示，将 `DataForm` 强转为对应的 `DolphinDB` 的数据形式。二者对应关系见下表：

| `GetDataForm` 返回值 | 实际类型   |
| -------------------- | ---------- |
| DfScalar(0)          | Scalar     |
| DfVector(1)          | Vector     |
| DfPair(2)            | Pair       |
| DfMatrix(3)          | Matrix     |
| DfSet(4)             | Set        |
| DfDictionary(5)      | Dictionary |
| DfTable(6)           | Table      |

该接口也提供了 `GetDataType` 方法获取数据类型的整型表示。目前支持获取 39 种数据类型的整型表示。二者对应关系见下表：

| `GetDataType` 返回值 | 实际类型   |
| -------------------- | ---------- |
| DtVoid(0)           | void     |
| DtBool(1)          | bool     |
| DtChar(2)            | char       |
| DtShort(3)          | short     |
| DtInt(4)             | int        |
| DtLong(5)      | long |
| DtDate(6)           | date      |
| DtMonth(7)           | month     |
| DtTime(8)          | time     |
| DtMinute(9)            | minute       |
| DtSecond(10)          | second     |
| DtDatetime(11)             | datetime        |
| DtTimestamp(12)      | timestamp |
| DtNanoTime(13)           | nanotime      |
| DtNanoTimestamp(14)           | nanotimestamp     |
| DtFloat(15)          | float     |
| DtDouble(16)            | double       |
| DtSymbol(17)          | symbol     |
| DtString(18)             | string        |
| DtUUID(19)      | uuid |
| DtFunction(20)           | function      |
| DtHandle(21)           | handle     |
| DtCode(22)          | code     |
| DtDatasource(23)            | datasource       |
| DtResource(24)          | resource     |
| DtAny(25)             | any        |
| DtCompress(26)      | compress |
| DtDictionary(27)           | dictionary      |
| DtDateHour(28)           | datehour     |
| DtDateMinute(29)          | dateminute     |
| DtIP(30)            | ipaddr       |
| DtInt128(31)          | int128     |
| DtBlob(32)             | blob        |
| DtComplex(34)      | complex |
| DtPoint(35)           | point      |
| DtDuration(36)           | duration     |
| DtDecimal32(37)          | decimal32     |
| DtDecimal64(38)            | decimal64       |
| DtDecimal128(39)          | decimal128     |
| DtObject(40)             | object        |

该接口还提供了 `GetDataTypeString` 方法获取数据类型的字符串表示。

Go API 提供的最核心的接口是 `DolphinDB`。Go API 通过它在 `DolphinDB` 服务器上执行脚本和函数，并在两者之间双向传递数据。通过 `NewDolphinDBClient` 或者 `NewSimpleDolphinDBClient` 初始化 `DolphinDB` 实例对象。该对象提供以下主要方法：

| 方法名                    | 详情                                            |
| ------------------------- | ----------------------------------------------- |
| Connect()       | 将会话连接到 DolphinDB 服务器                   |
| Login(l *LoginRequest)       | 登录服务器                                      |
| Logout()     | 登出服务器                                      |
| RunScript(script string)         | 将脚本在 DolphinDB 服务器运行                   |
| RunFile(fileName string)         | 读取文件中的脚本，将脚本在 DolphinDB 服务器运行 |
| RunFunc(s string, args []model.DataForm)    | 调用 DolphinDB 服务器上的函数                   |
| Upload(vars map[string]model.DataForm) | 将本地数据对象上传到 DolphinDB 服务器           |
| Close()                   | 关闭当前会话                                    |
| IsClosed()                | 判断会话是否关闭                                |
| GetSession()              | 获取当前会话的 SessionID                        |

还提供以下方法，进行数据库操作：

| 方法名                                      | 详情                                  |
| ------------------------------------------- | ------------------------------------- |
| ExistsDatabase(ExistsDatabaseRequest)       | 检查数据库是否存在                    |
| Database(DatabaseRequest)                   | 创建数据库                            |
| DropDatabase(DropDatabaseRequest)           | 删除数据库                            |
| ExistsTable(ExistsTableRequest)             | 检查表是否存在                        |
| Table(TableRequest)                         | 创建内存表                            |
| TableWithCapacity(TableWithCapacityRequest) | 创建指定容量的内存表                  |
| SaveTable(SaveTableRequest)                 | 保存表                                |
| LoadTable(LoadTableRequest)                 | 加载表                                |
| LoadText(LoadTextRequest)                   | 将数据文件加载到 DolphinDB 的内存表中 |
| SaveText(SaveTextRequest)                   | 保存文本                              |
| PloadText(PloadTextRequest)                 | 将数据文件并行加载到内存中            |
| LoadTableBySQL(LoadTableBySQLRequest)       | 通过 SQL 语句加载表                   |
| DropPartition(DropPartitionRequest)         | 删除数据库的指定分区数据              |
| DropTable(DropTableRequest)                 | 删除表                                |
| Undef(UndefRequest)                         | 取消定义指定对象                      |
| UndefAll()                                  | 取消定义所有对象                  |
| ClearAllCache(ClearAllCacheRequest)         | 清除所有缓存                          |

使用 `Database` 方法创建数据库后，会返回一个 `Database` 对象。该对象包含以下方法：

| 方法名                                                | 详情       |
| ----------------------------------------------------- | ---------- |
| CreateTable(CreateTableRequest)                       | 创建维度表 |
| CreatePartitionedTable(CreatePartitionedTableRequest) | 创建分区表 |

`Go API` 的实际用例参见 [example目录](./example)。

## 2. 安装依赖

使用 `go get` 下载安装 `Go API`

```sh
$ go get -u github.com/dolphindb/api-go
```

## 3. DolphinDB 使用示例

### 3.1. 初始化 DolphinDB

Go API 支持通过 `NewDolphinDBClient` 和 `NewSimpleDolphinDBClient` 两种方式来初始化 `DolphinDB` 实例：

1. NewDolphinDBClient 仅初始化客户端，需要通过 Connect 和 Login 去连接和登录服务端。该方法支持配置行为标识。

```go
package main

import (
    "context"

    "github.com/dolphindb/api-go/api"
)

func main() {
    host := "<ServerIP:Port>"
    // init client
    db, err := api.NewDolphinDBClient(context.TODO(), host, nil)
    if err != nil {
        // Handle exception
        panic(err)
    }

    // connect to server
    err = db.Connect()
    if err != nil {
        // Handle exception
        panic(err)
    }

    // init login request
    loginReq := &api.LoginRequest{
        UserID:   "userID",
        Password: "password",
    }

    // login dolphindb
    err = db.Login(loginReq)
    if err != nil {
        // Handle exception
        panic(err)
    }
}
```

2. NewSimpleDolphinDBClient 初始化客户端，并连接和登录服务端。该方法不支持配置行为标识。

```go
package main

import (
    "context"

    "github.com/dolphindb/api-go/api"
)

func main() {
    host := "<ServerIP:Port>"
    
    // new a client which has logged in the server
    db,err := api.NewSimpleDolphinDBClient(context.TODO(), host, "userID", "passWord")
    if err != nil {
        // Handle exception
        panic(err)
    }
}
```

### 3.2. 通过 API 建库建表

```go
package main

import (
    "context"

    "github.com/dolphindb/api-go/api"
)

func main() {
    host := "<ServerIP:Port>"
    db, err := api.NewSimpleDolphinDBClient(context.TODO(), host, "userID", "passWord")
    if err != nil {
        // Handle exception
        panic(err)
    }

    // init create database request
    dbReq := &api.DatabaseRequest{
        Directory:       "dfs://db1",
        PartitionType:   "VALUE",
        PartitionScheme: "1..10",
        DBHandle:        "example",
    }

    // create database
    dt, err := db.Database(dbReq)
    if err != nil {
        // Handle exception
        panic(err)
    }

    // init create partitioned table request
    createReq := &api.CreatePartitionedTableRequest{
        SrcTable:             "sourceTable",
        PartitionedTableName: "tableName",
        PartitionColumns:     []string{"id"},
    }

    // create partitioned table with database handler
    _, err = dt.CreatePartitionedTable(createReq)
    if err != nil {
        // Handle exception
        panic(err)
    }
}
```

### 3.3. 基础函数使用

#### 3.3.1. 构造数据类型

Go API 提供 `NewDataType` 方法构造数据类型对象，还提供 `NewDataTypeList`， `NewDataTypeListFromRawData` 以及 `NewEmptyDataTypeList` 方法构造数据类型数组，本节通过例子介绍常用数据类型及其数组的构造方法。
- 当存在可用的数据类型对象时，可以通过 `NewDataTypeList` 构造数据类型数组。
- 如果想用 go 类型构造数据类型数组，可以使用 `NewDataTypeListFromRawData` 方法，该方法的入参可以参考[对照表](#3312-newdatatypelistfromrawdata-入参对照表)。
- 如果想构造指定大小的空数据类型数组，可以使用 `NewEmptyDataTypeList` 方法。然后使用数据类型数组的 `Set` 或者 `SetWithRawData` 方法填充数组。

```go
package main

import (
   "fmt"

   "github.com/dolphindb/api-go/model"
)

// new a bool datatype variable
func main() {
   // new a string datatype variable
   dt, err := model.NewDataType(model.DtString, "sample")
   if err != nil {
        fmt.Println(err)
        return
   }
   
   // print value of variable with string format
   fmt.Println(dt.String())
   
   // print variable datatype
   fmt.Println(dt.DataType())
   
   // new datatypelist with datatype variable
   dl := model.NewDataTypeList(model.DtString, []model.DataType{dt})
   
   // print value of variable with string format
   fmt.Println(dl.StringList())
   
   // print number of elements
   fmt.Println(dl.Len())
   
   // print variable datatype
   fmt.Println(dt.DataType())
   
   // new datatypelist with basic type
   dl, err = model.NewDataTypeListFromRawData(model.DtString, []string{"sample", "test"})
   if err != nil {
        fmt.Println(err)
        return
   }
   
   // new a scalar object
   s := model.NewScalar(dt)
   
   fmt.Println(s)
   
   // new a vector object
   vct := model.NewVector(dl)
   
   fmt.Println(vct)
   
   // new a pair object
   p := model.NewPair(vct)
   
   fmt.Println(p)
   
   // new a matrix object
   data, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{2, 3, 4, 5, 6, 3, 4, 5, 6, 7, 4, 5, 6, 7, 8, 5, 6, 7, 8, 9, 6, 7, 8, 9, 10})
   if err != nil {
        fmt.Println(err)
        return
   }
   
   rowlabel, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{1, 2, 3, 4, 5})
   if err != nil {
        fmt.Println(err)
        return
   }
   
   colLabel, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{1, 2, 3, 4, 5})
   if err != nil {
        fmt.Println(err)
        return
   }
   
   m := model.NewMatrix(model.NewVector(data), model.NewVector(rowlabel), model.NewVector(colLabel))
   
   fmt.Println(m)
   
   // new a set object
   set := model.NewSet(vct)
   
   fmt.Println(set)
   
   // new a dictionary object
   keys, err := model.NewDataTypeListFromRawData(model.DtString, []string{"key1", "key2"})
   if err != nil {
        fmt.Println(err)
        return
   }
   
   values, err := model.NewDataTypeListFromRawData(model.DtString, []string{"value1", "value2"})
   if err != nil {
        fmt.Println(err)
        return
   }
   
   dict := model.NewDictionary(model.NewVector(keys), model.NewVector(values))
   
   fmt.Println(dict)
   
   // new a table object
   tb := model.NewTable([]string{"key"}, []*model.Vector{vct})
   
   fmt.Println(tb)
}
```

##### 3.3.1.1. NewDataType 入参对照表

| datatype                                                     | arg        |
| ------------------------------------------------------------ | ---------- |
| DtChar                                                       | byte       |
| DtBool,DtChar                                                | byte,bool  |
| DtBlob                                                       | []byte     |
| DtDecimal32                                                  | *model.Decimal32 |
| DtDecimal64                                                  | *model.Decimal64 |
| DtComplex,DtPoint                                            | [2]float64 |
| DtDouble                                                     | float64    |
| DtFloat                                                      | float32    |
| DtInt                                                        | int32      |
| DtLong                                                       | int64      |
| DtShort                                                      | int16      |
| DtTimestamp,DtMonth,DtSecond,DtNanoTimestamp,DtNanoTime,DtMinute,DtDatetime,DtDateHour,DtDate | time.Time  |
| DtAny                                                        | Dataform   |
| DtString,DtSymbol,DtUuid,DtIP,DtInt128,DtDuration            | string     |

* 注：当 datatype 为 DtBool 时，传入 0 表示 false，传入 NullBool 表示 Null，其他值表示 true。

##### 3.3.1.2. NewDataTypeListFromRawData 入参对照表

Golang 语法不允许一个数组里包含 nil，因此通过 Go API 传入包含空值的数组时，空值需填写为指定的方式，可以参考[Null 值对照表](#3313-null-值对照表)

| datatype                                                     | args         |
| ------------------------------------------------------------ | ------------ |
| DtChar                                                       | []byte       |
| DtBool                                                       | []byte,[]bool|
| DtBlob                                                       | [][]byte     |
| DtDecimal32                                                  | *model.Decimal32s |
| DtDecimal64                                                  | *model.Decimal64s |
| DtComplex,DtPoint                                            | [][2]float64 |
| DtDouble                                                     | []float64    |
| DtFloat                                                      | []float32    |
| DtInt                                                        | []int32      |
| DtLong                                                       | []int64      |
| DtShort                                                      | []int16      |
| DtTimestamp,DtMonth,DtSecond,DtNanoTimestamp,DtNanoTime,DtMinute,DtDatetime,DtDateHour,DtDate | []time.Time  |
| DtAny                                                        | []Dataform   |
| DtString,DtSymbol,DtUuid,DtIP,DtInt128,DtDuration            | []string     |

* 注：当 datatype 为 DtBool 且传入 byte 值时，传入 0 表示 false，传入 NullBool 表示 Null，其他值表示 true。

##### 3.3.1.3 Null 值对照表

| datatype                                                     | 空值         |
| ------------------------------------------------------------ | ------------ |
| DtBool                                                       | NullBool     |
| DtDecimal32                                                  | NullDecimal32Value     |
| DtDecimal64                                                  | NullDecimal64Value     |
| DtBlob                                                       | NullBlob     |
| DtChar                                                       | NullChar     |
| DtComplex                                                    | NullComplex  |
| DtDate,DtDateHour,DtDatetime,DtMinute,DtNanoTime,DtNanoTimestamp,DtSecond,DtMonth,DtTimestamp | NullTime     |
| DtDouble                                                     | NullDouble   |
| DtFloat                                                      | NullFloat    |
| DtDuration                                                   | NullDuration |
| DtInt                                                        | NullInt      |
| DtInt128                                                     | NullInt128   |
| DtIP                                                         | NullIP       |
| DtLong                                                       | NullLong     |
| DtPoint                                                      | NullPoint    |
| DtShort                                                      | NullShort    |
| DtUuid                                                       | NullUUID     |
| DtAny                                                        | NullAny      |
| DtString,DtSymbol                                            | NullString   |

使用示例

```go
_, err := model.NewDataTypeListFromRawData(model.DtBool, []byte{1, 0, model.NullBool})
if err != nil {
    fmt.Println(err)
    return
}

_, err = model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: 1, Value: []float64{10, model.NullDecimal32Value}})
if err != nil {
    fmt.Println(err)
    return
}

_, err = model.NewDataTypeListFromRawData(model.DtDecimal64, &model.Decimal64s{Scale: 1, Value: []float64{10, model.NullDecimal64Value}})
if err != nil {
    fmt.Println(err)
    return
}
```

#### 3.3.2. 完整示例

```go
package main

import (
    "context"
    "fmt"

    "github.com/dolphindb/api-go/api"
    "github.com/dolphindb/api-go/model"
)

func main() {
    host := "<ServerIP:Port>"
    db, err := api.NewSimpleDolphinDBClient(context.TODO(), host, "userID", "passWord")
    if err != nil {
        // Handle exceptions
        panic(err)
    }

    // run script on dolphindb server
    raw, err := db.RunScript("schema(tablename)")
    if err != nil {
        // Handle err
        panic(err)
    }

    // print the real dataform
    fmt.Println(raw.GetDataForm())

    // get the variable with real type
    dict := raw.(*model.Dictionary)
    fmt.Println(dict)

    // declare the specified variable ont the server
    _, err = db.Upload(map[string]model.DataForm{"dict": dict})
    if err != nil {
        // Handle exception
        panic(err)
    }

    // run function on dolphindb server
    _, err = db.RunFunc("typestr", []model.DataForm{dict})
    if err != nil {
        // Handle exception
        panic(err)
    }
}
```


### 3.4. 初始化 DBConnectionPool

`DBConnectionPool` 可以复用多个 Connection。可以直接使用 `DBConnectionPool` 的 `Execute` 方法执行任务，然后使用 `Task` 的 `GetResult` 方法获取该任务的执行结果。

| 方法名                               | 详情               |
| :----------------------------------- | :----------------- |
| NewDBConnectionPool(opt *PoolOption) | 初始化连接池对象   |
| Execute(tasks []*Task)               | 执行批量任务       |
| GetPoolSize()                        | 获取连接数         |
| Close()                              | 关闭连接池         |
| IsClosed()                           | 检查连接池是否关闭 |

PoolOption 参数说明：

* Address：字符串，表示所连接的服务器的地址。
* UserID / Password: 字符串，登录时的用户名和密码。
* PoolSize：整数，表示连接池的容量。
* LoadBalance：布尔值，表示是否开启负载均衡，开启后会根据各个数据节点的地址来创建连接池。
* LoadBalanceAddresses: 字符串数组，用于指定数据节点。

`Task` 封装了查看任务执行结果的相关方法。

| 方法名      | 详情                     |
| :---------- | :----------------------- |
| IsSuccess() | 任务是否执行成功         |
| GetResult() | 获取脚本运行结果         |
| GetError()  | 获取任务运行时发生的错误 |

建立一个 `DBConnectionPool` 连接数为10的连接池。

```go
poolOpt := &api.PoolOption{
    Address:  "ServerIP:Port",
    UserID:   "UserID",
    Password: "Password",
    PoolSize: 10,
}

pool, err := api.NewDBConnectionPool(poolOpt)
if err != nil {
    fmt.Println(err)
    return
}
```

创建一个任务。

```go
task := &api.Task{Script: "1..10"}
err = pool.Execute([]*api.Task{task})
if err != nil {
    fmt.Println(err)
    return
}
```

检查任务是否执行成功。如果执行成功，获取相应结果；如果失败，获取错误。

```go
var data *model.Vector
if task.IsSuccess() {
    data = task.GetResult()
    fmt.Println(data)
} else {
    fmt.Println(task.GetError())
}
```

输出

```
vector<int>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
```

创建多个任务，在 `DBConnectionPool` 上并行调用。

```go
tasks := make([]*api.Task, 10)
for i := 0; i < 10; i++ {
    tasks[i] = &api.Task{
        Script: "log",
        Args:   []model.DataForm{model.NewScalar(data.Get(i))},
    }
}

err = pool.Execute(tasks)
if err != nil {
    fmt.Println(err)
    return
}
```

检查任务是否都执行成功。如果执行成功，获取相应结果；如果失败，获取错误。

```go
for _, v := range tasks {
    if v.IsSuccess() {
        fmt.Println(v.GetResult().String())
    } else {
        fmt.Println(v.GetError())
    }
}
```

输出

```go
double(0)
double(0.6931471805599453)
double(1.0986122886681096)
double(1.3862943611198906)
double(1.6094379124341003)
double(1.791759469228055)
double(1.9459101490553132)
double(2.0794415416798357)
double(2.1972245773362196)
double(2.302585092994046)
```

## 4. 读写 DolphinDB 数据表

`DolphinDB` 数据表按存储方式分为两种:

- 内存表: 数据仅保存在内存中，存取速度最快。仅当前会话可见，需要通过 `share` 在会话间共享内存表，以便其它会话可以访问。可以直接使用表名加载。
- 分布式表：数据分布在不同的节点，通过 DolphinDB 的分布式计算引擎，逻辑上仍然可以像本地表一样做统一查询。是 `DolphinDB` 推荐在生产环境下使用的数据存储方式，它支持快照级别的事务隔离，保证数据一致性。分布式表支持多副本机制，既提供了数据容错能力，又能作为数据访问的负载均衡。需要使用 loadTable 加载。

下面统一使用分布式表作为示例。

### 4.1. 读取和使用数据表

#### 4.1.1. 读取数据表

在 Go API 中使用如下代码可读取分布式表数据。

```go
dbPath := "dfs://testDatabase"
tbName := "tb1"
conn, err := api.NewSimpleDolphinDBClient(context.TODO(), "ServerIP:Port", "admin", "123456")
if err != nil {
    fmt.Println(err)
    return
}

tb, err := conn.RunScript(fmt.Sprintf("select * from loadTable('%s','%s') where cdate = 2017.05.03", dbPath, tbName))
if err != nil {
    fmt.Println(err)
    return
}
```

#### 4.1.2. 使用 Table 对象

Go API 通过 `Table` 对象来存储数据表。`Table` 对象采用列式存储，无法直接读取行数据，因此需要先读取列，再读取行。  
以表对象 t 为例，其包含4个列，列名分别为cstring, cint, ctimestamp, cdouble，数据类型分别是STRING, INT, TIMESTAMP, DOUBLE。通过 Go API 分别打印 t 中每个列的列名和对应的值。

```go
for _, v := range t.GetColumnNames() {
    fmt.Println("ColumnName: ", v)
    col := table.GetColumnByName(v)
    fmt.Println("ColumnValue: ", col.String())
}
```

### 4.2. 保存数据到 DolphinDB 数据表

DolphinDB 提供多种脚本语句来保存数据到内存表：

- 通过 `insert into` 追加单行数据
- 通过 `tableInsert` 函数向表中批量追加数组对象
- 通过 `tableInsert` 函数向表中追加 `Table` 对象
- 通过 `append!` 追加数据到内存表

Go API 可以通过 `RunScript` 接口将插入数据的脚本发送至服务端执行。

#### 4.2.1. 同步追加数据

下面分别介绍三种方式保存数据的实例:

通过 [GUI](https://github.com/dolphindb/Tutorials_EN/blob/master/gui_tutorial.md) 在 DolphinDB server 端创建一个分布式表。

```sql
dbPath = 'dfs://testDatabase'
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```

##### 4.2.1.1. 使用 `insert into` 追加单行数据

若将单条数据记录保存到 DolphinDB 数据表，可以使用 `insert into`。

```go
func testSaveInsert(str string, i int, ts int64, dbl float64, db api.DolphinDB) {
    df, err := db.RunScript(fmt.Sprintf("insert into loadTable('dfs://testDatabase','tb1') values('%s',%d,%d,%f)", str, i, ts, dbl))
	if err != nil {
		fmt.Println(err)
		return
	}

    fmt.Println(df)
}
```

##### 4.2.1.2. 使用 `tableInsert` 函数向表中批量追加数组对象

`tableInsert` 可将多个数组追加到 `DolphinDB` 数据表中，比较适合用来批量保存数据。

```go
func testTableInsert(strVector, intVector, timestampVector, doubleVector *model.Vector, db api.DolphinDB) {
    args := make([]model.DataForm, 4)
    args[0] = strVector
    args[1] = intVector
    args[2] = timestampVector
    args[3] = doubleVector
    df, err := db.RunFunc("tableInsert{loadTable('dfs://testDatabase','tb1')}", args)
    if err != nil {
    	fmt.Println(err)
    	return
    }
    
    fmt.Println(df)
}
```

在本例中，使用了 `DolphinDB` 中的[部分应用](https://www.dolphindb.cn/cn/help/200/Functionalprogramming/PartialApplication.html)这一特性，将服务端表名以 `tableInsert{loadTable('dfs://testDatabase','tb1')}` 的方式固化到 `tableInsert` 中，作为一个独立函数来使用。

##### 4.2.1.3. 使用 `tableInsert` 函数向表中追加 `Table` 对象

`tableInsert` 函数也可以接受一个表对象作为参数，批量添加数据。Go API 将获取的数据处理后组织成 `Table` 对象后，通过 `tableInsert` 插入 `DolphinDB` 数据表。

```go
func testTableInsert(tableObj *model.Table, db api.DolphinDB) {
    args := make([]model.DataForm, 1)
    args[0] = tableObj
    df, err := db.RunFunc("tableInsert{loadTable('dfs://testDatabase','tb1')}", args)
    if err != nil {
        fmt.Println(err)
        return
    }

    fmt.Println(df)
}
```

#### 4.2.2. 异步追加数据

##### 4.2.2.1. 数据表的并发写入

`DolphinDB` 的数据表支持并发读写，Go API 提供 `PartitionedTableAppender` 来支持数据表的并发写入，仅支持按表写入。

使用 1.30 版本以上的 server，可以通过 Go API 中的 PartitionedTableAppender 来写入分布式表。其基本原理是设计一个连接池用于多协程写入，将写入的数据按指定的分区列进行分类，并分别放入不同的连接并行写入。

下面展示如何在 Go 客户端中将数据并发写入 `DolphinDB` 的分布式表。

首先，在 `DolphinDB` 服务端执行以下脚本，创建分布式数据库 `"dfs://demohash"` 和分布式表 `"pt"`。其中，数据库按照 `HASH-HASH` 的组合进行二级分区。

```sql
t = table(timestamp(1..10) as date,string(1..10) as sym)
db1=database("",HASH,[DATETIME,10])
db2=database("",HASH,[STRING,5])
if(existsDatabase("dfs://demohash")){
    dropDatabase("dfs://demohash")
}
db = database("dfs://demohash",COMPO,[db2,db1])
pt = db.createPartitionedTable(t,`pt,`sym`date)
```

然后，使用 Go API 初始化 `PartitionedTableAppender` 对象

```go
poolOpt := &api.PoolOption{
    Address:     "ServerIP:Port",
    UserID:      "admin",
    Password:    "123456",
    PoolSize:    3,
    LoadBalance: true,
}

pool, err := api.NewDBConnectionPool(poolOpt)
if err != nil {
    fmt.Println(err)
    return
}

appenderOpt := &api.PartitionedTableAppenderOption{
    Pool:             pool,
    DBPath:           "dfs://demohash",
    TableName:        "pt",
    PartitionCol:     "sym",
}

appender, err := api.NewPartitionedTableAppender(appenderOpt)
if err != nil {
    fmt.Println(err)
    return
}
```

`PartitionedTableAppenderOption` 参数说明：
* Pool: 表示连接池。
* DBPath: 字符串，数据库路径。
* TableName：字符串，表示数据表名。
* PartitionCol：字符串，表示分布式表列名。
* AppendFunction: 可选，自定义写入函数名，不填此参数则调用内置 tableInsert 函数。

最后，将数据插入到数据表中：

```go 
colNames := []string{"sym", "date"}

sym, err := model.NewDataTypeListFromRawData(model.DtString, []string{"sample", "test"})
if err != nil {
    fmt.Println(err)
    return
}

date, err := model.NewDataTypeListFromRawData(model.DtDatetime, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)})
if err != nil {
    fmt.Println(err)
    return
}

col1 := model.NewVector(sym)
col2 := model.NewVector(date)

m, err := appender.Append(model.NewTable(colNames, []*model.Vector{col1, col2}))
if err != nil {
    fmt.Println(err)
    return
}

fmt.Println(m)
```

### 4.3. 批量异步追加数据

DolphinDB Go API 提供 `MultiGoroutineTable` 对象用于批量异步追加数据，并在客户端维护了一个数据缓冲队列。当服务器端忙于网络 I/O 时，客户端写协程仍然可以将数据持续写入缓冲队列（该队列由客户端维护）。写入队列后即可返回，从而避免了写协程的忙等。目前，`MultiGoroutineTable` 支持批量写入数据到内存表、分区表和维度表。

注意对于异步写入：

* 支持按行向表中追加数据。
* API 客户端提交任务到缓冲队列，缓冲队列接到任务后，客户端即认为任务已完成。
* 提供 `GetStatus` 方法查看状态。

#### 4.3.1. MultiGoroutineTable

`MultiGoroutineTable` 支持多协程的并发写入。

`MultiGoroutineTable` 对象初始化如下：

```go
opt := &multigoroutinetable.Option{
    Database:       "dbName",
    Address:        "ServerIP:Port",
    UserID:         "admin",
    Password:       "123456",
    TableName:      "tbName",
    GoroutineCount: 2,
    PartitionCol:   "colName",
    BatchSize:      1000,
    Throttle:       1,
}

writer, err := multigoroutinetable.NewMultiGoroutineTable(opt)
if err != nil {
    fmt.Println(err)
    return
}
```

Option 参数说明：

* Address 字符串，表示所连接的服务器的地址.
* UserID / Password: 字符串，登录时的用户名和密码。
* Database: 字符串，表示数据库的路径或句柄。如果是内存表，则无需设置该参数。
* TableName 字符串，表示表的名称。
* BatchSize 整数，表示批处理的消息的数量。如果该参数值为 1，表示客户端写入数据后就立即发送给服务器；
  如果该参数大于 1，表示数据量达到 BatchSize 时，客户端才会将数据发送给服务器。
* Throttle 大于 0 的整数，单位为毫秒。若客户端有数据写入，但数据量不足 BatchSize，则等待 Throttle 的时间再发送数据。
* GoroutineCount 整数，表示创建的工作协程数量，如果值为 1，表示单协程。对于维度表，其值必须为 1。
* PartitionCol 字符串类型，默认为空，仅在 GoroutineCount 大于1时起效。对于分区表，必须指定为分区字段名；
  如果是内存表，必须指定为表的字段名；对于维度表，该参数不起效。

以下是 `MultiGoroutineTable` 对象包含的函数方法介绍：

```go
Insert(args ...interface{}) error
```

函数说明：

插入单行数据。返回一个 error 对象。
数据类型需要与表的列存储的数据类型一致，或者为列存储的数据类型的基础类型，具体可参考[可用入参对照表](#3311-newdatatype-的可用入参对照表)
因写入是异步操作，所以当 error 为 nil 时，不代表写入操作成功。
写入操作是否成功可以打印 `GetStatus` 方法返回的对象。

参数说明：

* args: 是变长参数，代表插入一行数据

示例：

```go
err = writer.Insert("2", time.Date(2022, time.Month(1), 1, 1, 1, 0, 0, time.UTC))
```

```go
GetUnwrittenData() [][]model.DataType
```

函数说明：

返回一个嵌套列表，表示未写入服务器的数据。

注意：该方法获取到数据资源后，`MultiGoroutineTable` 将释放这些数据资源。

示例：

```go
unwrittenData := writer.GetUnwrittenData()
```

```go
InsertUnwrittenData(records [][]model.DataType) error
```

函数说明：

将数据插入数据表。返回值同 insert 方法。与 insert 方法的区别在于，insert 只能插入单行数据，而 insertUnwrittenData 可以同时插入多行数据。

参数说明：

* records：需要再次写入的数据。可以通过方法 GetUnwrittenData 获取该对象。

示例：

```go
err = writer.InsertUnwrittenData(unwrittenData)
```

```go
GetStatus() *Status
```

函数说明：

获取 `MultiGoroutineTable` 对象当前的运行状态。

返回参数说明：

* Status：包含 MultiGoroutineTable 执行状态的对象

示例：

```go
status := writer.GetStatus()
```


status 属性：

* IsExit：写入协程是否正在退出。
* ErrMsg：错误信息。
* SentRows：成功发送的总记录数。
* UnsentRows：待发送的总记录数。
* FailedRows：发送失败的总记录数。
* GoroutineStatus：写入协程状态列表。
  - GoroutineIndex：协程索引。
  - SentRows：该协程成功发送的记录数。
  - UnsentRows：该协程待发送的记录数。
  - FailedRows：该协程发送失败的记录数。

```go
WaitForGoroutineCompletion()
```

函数说明：

调用此方法后，`MultiGoroutineTable` 会进入等待状态，待后台工作协程全部完成后退出等待状态。

示例：

```go
writer.WaitForGoroutineCompletion()
```

MultiGoroutineTable 的正常使用示例如下：

```go
conn, err := api.NewSimpleDolphinDBClient(context.TODO(), "ServerIP:Port", "admin", "123456")
if err != nil {
    return
}

buf := bytes.NewBufferString("dbName = 'dfs://valuedb3'\n")
buf.WriteString("if (exists(dbName)){dropDatabase(dbName);}\n")
buf.WriteString("datetest = table(1000:0,`date`symbol`id,[DATE, SYMBOL, LONG]);\n")
buf.WriteString("db = database(directory= dbName, partitionType= HASH, partitionScheme=[INT, 10]);")
buf.WriteString("pt = db.createPartitionedTable(datetest,'pdatetest','id');")
_, err = conn.RunScript(buf.String())
if err != nil {
    return
}

opt := &multigoroutinetable.Option{
    Database:       "dfs://valuedb3",
    Address:        "ServerIP:Port",
    UserID:         "admin",
    Password:       "123456",
    TableName:      "pdatetest",
    GoroutineCount: 5,
    PartitionCol:   "id",
    BatchSize:      10000,
    Throttle:       1,
}

writer, err := multigoroutinetable.NewMultiGoroutineTable(opt)
if err != nil {
    return
}

// insert 100 row data
for ind := 0; ind < 100; ind++ {
    err = writer.Insert(time.Date(2022, time.Month(1), 1, 1, 1, 0, 0, time.UTC), "AAAAAAAAAB", rand.Int63()%10000)
    if err != nil {
        return
    }
}

// wait for insertion to complete
writer.WaitForGoroutineCompletion()

status := writer.GetStatus()
fmt.Println("writeStatus: \n", status)

raw, err := conn.RunScript("exec count(*) from pt")
if err != nil {
    return
}

fmt.Println(raw)
```

以上代码输出结果为

```sh
"""
writeStatus:  
errMsg         :  
isExit         :  false
sentRows       :  100
unsentRows     :  0
sendFailedRows :  0
goroutineStatus   :
    goroutineIndex: 0, sentRows: 18, unsentRows: 0, sendFailedRows: 0
    goroutineIndex: 1, sentRows: 23, unsentRows: 0, sendFailedRows: 0
    goroutineIndex: 2, sentRows: 19, unsentRows: 0, sendFailedRows: 0
    goroutineIndex: 3, sentRows: 20, unsentRows: 0, sendFailedRows: 0
    goroutineIndex: 4, sentRows: 20, unsentRows: 0, sendFailedRows: 0

long(100)
"""
```

**注意**：使用 writer.WaitForGoroutineCompletion() 方法等待 `MultiGoroutineTable` 写入完毕，会终止 `MultiGoroutineTable` 所有工作协程，保留最后一次写入信息。此时如果需要再次将数据写入 `MultiGoroutineTable`，需要重新获取新的 `MultiGoroutineTable` 对象，才能继续写入数据。

由上例可以看出，`MultiGoroutineTable` 内部使用多协程完成数据转换和写入任务。但在 `MultiGoroutineTable` 外部，API 客户端同样支持以多协程方式将数据写入 `MultiGoroutineTable`，且保证了多协程安全。

#### 4.3.2. MultiGoroutineTable 常见错误

MultiGoroutineTable 调用 Insert 方法插入数据时出错：

- 在调用 MultiGoroutineTable 的 Insert 方法时，若插入数据的类型与表对应列的类型不匹配，则 MultiGoroutineTable 会立刻返回错误信息。

示例：

```go
conn, err := api.NewSimpleDolphinDBClient(context.TODO(), "ServerIP:Port", "admin", "123456")
if err != nil {
    return
}

buf := bytes.NewBufferString("dbName = 'dfs://valuedb3'\n")
buf.WriteString("if (exists(dbName)){dropDatabase(dbName);}\n")
buf.WriteString("datetest = table(1000:0,`date`symbol`id,[DATE, SYMBOL, LONG]);\n")
buf.WriteString("db = database(directory= dbName, partitionType= HASH, partitionScheme=[INT, 10]);")
buf.WriteString("pt = db.createPartitionedTable(datetest,'pdatetest','id');")
_, err = conn.RunScript(buf.String())
if err != nil {
    return
}

opt := &multigoroutinetable.Option{
    Database:       "dfs://valuedb3",
    Address:        "ServerIP:Port",
    UserID:         "admin",
    Password:       "123456",
    TableName:      "pdatetest",
    GoroutineCount: 5,
    PartitionCol:   "id",
    BatchSize:      10000,
    Throttle:       1,
}

writer, err := multigoroutinetable.NewMultiGoroutineTable(opt)
if err != nil {
    return
}

// insert data with wrong type
err = writer.Insert(time.Date(2022, time.Month(1), 1, 1, 1, 0, 0, time.UTC), 222, rand.Int63()%10000)
if err != nil {
    fmt.Println(err)
    return
}
```

以上代码输出结果为：

```go
"""
the type of in must be string when datatype is DtString, DtCode, DtFunction, DtHandle or DtSymbol
"""
```

- 在调用 MultiGoroutineTable 的 Insert 方法时，若 Insert 插入数据的列数和表的列数不匹配，MultiGoroutineTable 会立刻返回错误信息。

示例：

```go
conn, err := api.NewSimpleDolphinDBClient(context.TODO(), "ServerIP:Port", "admin", "123456")
if err != nil {
    return
}

buf := bytes.NewBufferString("dbName = 'dfs://valuedb3'\n")
buf.WriteString("if (exists(dbName)){dropDatabase(dbName);}\n")
buf.WriteString("datetest = table(1000:0,`date`symbol`id,[DATE, SYMBOL, LONG]);\n")
buf.WriteString("db = database(directory= dbName, partitionType= HASH, partitionScheme=[INT, 10]);")
buf.WriteString("pt = db.createPartitionedTable(datetest,'pdatetest','id');")
_, err = conn.RunScript(buf.String())
if err != nil {
    return
}

opt := &multigoroutinetable.Option{
    Database:       "dfs://valuedb3",
    Address:        "ServerIP:Port",
    UserID:         "admin",
    Password:       "123456",
    TableName:      "pdatetest",
    GoroutineCount: 5,
    PartitionCol:   "id",
    BatchSize:      10000,
    Throttle:       1,
}

writer, err := multigoroutinetable.NewMultiGoroutineTable(opt)
if err != nil {
    return
}

// insert data with more data
err = writer.Insert(time.Date(2022, time.Month(1), 1, 1, 1, 0, 0, time.UTC), rand.Int63()%10000)
if err != nil {
    fmt.Println(err)
    return
}
```

以上代码输出结果为：

```sh
"""
Column counts don't match 
"""
```

如果 MultiGoroutineTable 在运行时连接断开，则所有工作协程被终止。继续通过 MultiGoroutineTable 向服务器写数据时，会因为工作协程终止而报错，且数据不会被写入。此时，可通过调用 MultiGoroutineTable 的 GetUnwrittenData 获取未插入的数据，并重新插入。

示例：

```go
conn, err := api.NewSimpleDolphinDBClient(context.TODO(), "ServerIP:Port", "admin", "123456")
if err != nil {
    return
}

buf := bytes.NewBufferString("dbName = 'dfs://valuedb3'\n")
buf.WriteString("if (exists(dbName)){dropDatabase(dbName);}\n")
buf.WriteString("datetest = table(1000:0,`date`symbol`id,[DATE, SYMBOL, LONG]);\n")
buf.WriteString("db = database(directory= dbName, partitionType= HASH, partitionScheme=[INT, 10]);")
buf.WriteString("pt = db.createPartitionedTable(datetest,'pdatetest','id');")
_, err = conn.RunScript(buf.String())
if err != nil {
    return
}

opt := &multigoroutinetable.Option{
    Database:       "dfs://valuedb3",
    Address:        "ServerIP:Port",
    UserID:         "admin",
    Password:       "123456",
    TableName:      "pdatetest",
    GoroutineCount: 5,
    PartitionCol:   "id",
    BatchSize:      10000,
    Throttle:       1,
}

writer, err := multigoroutinetable.NewMultiGoroutineTable(opt)
if err != nil {
    return
}

// insert data with more data
err = writer.Insert(time.Date(2022, time.Month(1), 1, 1, 1, 0, 0, time.UTC), rand.Int63()%10000)
if err != nil {
    fmt.Println(err)
    return
}

unwriterdata := writer.GetUnwrittenData()
fmt.Println("unWriterdata: ", len(unwriterdata))

// renew MultiGoroutineTable object
writer, err = multigoroutinetable.NewMultiGoroutineTable(opt)
if err != nil {
    return
}

err = writer.InsertUnwrittenData(unwriterdata)
if err != nil {
    return
}

// wait for insertion to complete
writer.WaitForGoroutineCompletion()

status := writer.GetStatus()
fmt.Println("writeStatus: \n", status)
```

以上代码输出结果为：

```go
"""
unWriterdata: 10
writeStatus:  
errMsg         :  
isExit         :  true
sentRows       :  10
unsentRows     :  0
sendFailedRows :  0
goroutineStatus   :
    goroutineIndex: 0, sentRows: 3, unsentRows: 0, sendFailedRows: 0
    goroutineIndex: 1, sentRows: 2, unsentRows: 0, sendFailedRows: 0
    goroutineIndex: 2, sentRows: 1, unsentRows: 0, sendFailedRows: 0
    goroutineIndex: 3, sentRows: 3, unsentRows: 0, sendFailedRows: 0
    goroutineIndex: 4, sentRows: 1, unsentRows: 0, sendFailedRows: 0
"""
```

## 5. 流数据 API

Go API 可以通过 API 订阅流数据。有三种创建订阅客户端的方式：单协程回调（GoroutineClient），多协程回调（GoroutinePooledClient）和通过 PollingClient 返回的对象获取消息队列.

### 5.1. 代码示例:

在调用订阅函数之前，需要先封装 SubscribeRequest 对象:
`SubscribeRequest` 参数说明:

* Address: 发布端节点的地址
* TableName：发布表的名称
* ActionName：订阅任务的名称
* BatchSize: 整数，表示批处理的消息的数量。如果它是正数，直到消息的数量达到 batchSize 时，Handler 才会处理进来的消息。如果它没有指定或者是非正数，消息到达之后，Handler 就会马上处理消息。仅对 GoroutineClient 客户端有效。
* Offset: 整数，表示订阅任务开始后的第一条消息所在的位置。消息是流数据表中的行。如果没有指定 offset，或它为负数或超过了流数据表的记录行数，订阅将会从流数据表的当前行开始。offset 与流数据表创建时的第一行对应。如果某些行因为内存限制被删除，在决定订阅开始的位置时，这些行仍然考虑在内
* AllowExists: 当 AllowExists = true 时，若已存在的订阅被再次订阅，不会抛出异常。默认值为 false
* Throttle: 浮点数，表示 Handler 处理到达的消息之前等待的时间，以秒为单位。默认值为 1。如果没有指定 BatchSize，Throttle 将不会起作用。仅对 GoroutineClient 客户端有效。
* Reconnect: 布尔值，表示订阅中断后，是否会自动重订阅
* Filter: 一个向量，表示过滤条件。流数据表过滤列在 filter 中的数据才会发布到订阅端，不在 filter 中的数据不会发布
* Handler：用户自定义的回调函数，用于处理每次流入的数据，仅在支持回调的订阅客户端可用

下面分别介绍如何通过3种方法订阅流数据

- 通过客户机上的应用程序定期去流数据表查询是否有新增数据，推荐使用 PollingClient

```go
client := streaming.NewPollingClient("localhost", 8101)
req := &streaming.SubscribeRequest{
    Address:    "ServerIP:Port",
    TableName:  "pub1",
    ActionName: "action1",
    Offset:     0,
    Reconnect:  true,
}

poller, err := client.Subscribe(req)
if err != nil {
    return
}

msgs := poller.Poll(1000, 1000)
fmt.Println(msgs)
```

poller 探测到流数据表有新增数据后，会拉取到新数据。无新数据发布时，程序会阻塞在 `poller.Poll` 方法, 直到超时。

- 使用 MessageHandler 回调的方式获取新数据

首先需要调用者定义数据处理器 Handler。Handler 需要实现 `streaming.MessageHandler` 接口。

```go
type sampleHandler struct{}

func (s *sampleHandler) DoEvent(msg streaming.IMessage) {
	// do something
}
```

在启动订阅时，把 Handler 实例作为参数传入订阅函数。包括单协程回调和多协程回调两种方式。

 1. 单协程回调 GoroutineClient
 GoroutineClient 在接收到多条订阅信息时，会调用 Handler 的 DoEvent 方法，顺序处理订阅信息。

```go
client := streaming.NewGoroutineClient("localhost", 8100)
req := &streaming.SubscribeRequest{
    Address:    "ServerIP:Port",
    TableName:  "pub",
    ActionName: "action1",
    Handler:    new(sampleHandler),
    Offset:     0,
    Reconnect:  true,
}

err := client.Subscribe(req)
if err != nil {
    return
}
```

当流数据表有新增数据时， Go API 会自动调用 sampleHandler 的 DoEvent 方法。

 2. 多协程回调 GoroutinePooledClient
GoroutinePooledClient 在接收到多条订阅信息时，并发调用 Handler 的 DoEvent 方法。需额外加锁保证 DoEvent 的并发安全。

```go
client := streaming.NewGoroutinePooledClient("localhost", 8100)
req := &streaming.SubscribeRequest{
    Address:    "ServerIP:Port",
    TableName:  "pub",
    ActionName: "action1",
    Handler:    new(sampleHandler),
    Offset:     0,
    Reconnect:  true,
}

err := client.Subscribe(req)
if err != nil {
    return
}
```

*注* 使用 GoroutinePooledClient 订阅流数据，无法保证订阅消息的处理顺序。

### 5.2. 断线重连

`Reconnect` 参数是一个布尔值，表示订阅意外中断后，是否会自动重新订阅。默认值为 false。

若 `Reconnect` 设置为 true 时，订阅意外中断后系统是否以及如何自动重新订阅，取决于订阅中断由哪种原因导致：

- 如果发布端与订阅端处于正常状态，但是网络中断，那么订阅端会在网络正常时，自动从中断位置重新订阅。
- 如果发布端崩溃，订阅端会在发布端重启后不断尝试重新订阅。
    - 如果发布端对流数据表启动了持久化，发布端重启后会首先读取硬盘上的数据，直到发布端读取到订阅中断位置的数据，订阅端才能成功重新订阅。
    - 如果发布端没有对流数据表启用持久化，那么订阅端将自动重新订阅失败。
- 如果订阅端崩溃，订阅端重启后不会自动重新订阅，需要重新执行 `Subscribe` 函数。

### 5.3. 启用 Filter

`Filter` 参数是一个向量。该参数需要发布端配合 `setStreamTableFilterColumn` 函数一起使用。使用 `setStreamTableFilterColumn` 指定流数据表的过滤列，流数据表过滤列在 filter 中的数据才会发布到订阅端，不在 `Filter` 中的数据不会发布。

以下例子将一个包含元素1和2的整数类型向量作为 `Subscribe` 的 Filter 参数：

```go
dtl, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{1, 2})
if err != nil {
    return
}

client := streaming.NewPollingClient("localhost", 8101)
req := &streaming.SubscribeRequest{
    Address:    "ServerIP:Port",
    TableName:  "pub1",
    ActionName: "action1",
    Offset:     0,
    Reconnect:  true,
    Filter:     model.NewVector(dtl),
}

_, err = client.Subscribe(req)
if err != nil {
    return
}
```

### 5.4. 取消订阅

每一个订阅都有一个订阅主题 `topic` 作为唯一标识。如果订阅时 `topic` 已经存在，那么会订阅失败。这时需要通过 `UnSubscribe` 函数取消订阅才能再次订阅。
```go
err = client.UnSubscribe(req)
if err != nil {
    return
}
```

## 6. 工具方法

### 6.1 model 包

#### GetDataTypeString

```go
GetDataTypeString(t DataTypeByte) string
```

函数说明：

根据传入的 t，得到数据类型的字符串表示

示例：
```go
dts := model.GetDataTypeString(model.DtString)
fmt.Println(dts)
```

#### GetDataFormString

```go
GetDataFormString(t DataFormByte) string
```

函数说明：

根据传入的 t，得到数据形式的字符串表示

示例：
```go
dfs := model.GetDataFormString(model.DfTable)
fmt.Println(dfs)
```

#### NewTableFromStruct

```go
NewTableFromStruct(obj interface{}) (*Table, error)
```

函数说明：

将传入的 obj 转成 Table 对象。

入参说明：

obj 可以为任意结构体对象，但是该结构体字段类型需要为数组，且字段带有以 `dolphindb` 开头的特定 tag，如 `dolphindb:"column:name,type:string"`。
其中，column 后为列名，type 后为列数据类型(通过 model.GetDataTypeString 获取)。字段类型需要跟列数据类型匹配。

示例：
```go
type Example struct {
    Name []string `dolphindb:"column:name,type:string"`
}

func main() {
    val := &Example{
        Name: []string{"Jane","BOB"},
    }

    tb,err := model.NewTableFromStruct(val)
    if err != nil {
        fmt.Println(err)
        return
    }
    fmt.Println(tb)
}
```

#### NewTableFromRawData

```go
NewTableFromRawData(colNames []string, colTypes []DataTypeByte, colValues []interface{}) (*Table, error)
```

函数说明：

根据传入的 colNames, colTypes 和 colValues, 生成一个 Table 对象。
colNames 为表中列名的数组。
colTypes 为表中列类型的数组。
colValues 为表中列值的数组，可以根据[对照表](#3312-newdatatypelistfromrawdata-入参对照表)来赋值。

示例：
```go
func main() {
    colNames := []string{"name", "id"}
	colTypes := []model.DataTypeByte{model.DtString, model.DtInt}
	colValues := []interface{}{[]string{"Tom", "Bob"}, []int32{1, 2}}
	tb, err := model.NewTableFromRawData(colNames, colTypes, colValues)
    if err != nil {
        return
    }
	fmt.Println(tb)
}
```