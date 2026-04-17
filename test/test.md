# `test` 目录可执行改进清单

下面按文件给出优先级、问题点和建议补测方向。这里先不改代码，只整理成后续可以直接执行的清单。

## P0: 先处理的基础问题

### `test/setup/settings.go`
- 问题：环境地址、端口、控制节点地址全部硬编码，测试只能在固定集群跑。
- 问题：`HA_sites` 直接绑定固定节点集合，导致不同环境下测试选择不可配置。
- 建议补测：增加一组“空配置/非法配置/缺失配置”场景，验证测试启动前就能识别配置问题。
- 建议补测：增加一组通过环境变量或测试参数覆盖地址的场景，确认测试不再依赖单一集群。

### `test/connectionPool_test.go`
- 问题：文件内容最重，既有连接池基础测试，又有 HA、重连、负载均衡、连接数统计，职责混杂。
- 问题：存在多个 `SkipConvey` / `SkipNow`，默认覆盖率偏低。
- 问题：大量 `time.Sleep`，尤其是 HA 节点停启和重连流程，测试慢且不稳定。
- 问题：部分测试忽略错误返回，或者只验证“没报错”，没有验证实际切换目标。
- 建议补测：
  - 连接池创建失败：`PoolSize<=0`、地址非法、`Timeout<0`、`EnableHighAvailability=true` 但 `HighAvailabilitySites` 为空。
  - 连接池生命周期：`Close()` 重复调用、`Execute()` 在关闭后调用、`IsClosed()` 的状态转换。
  - HA 负向场景：首选节点不可用、备选节点也不可用、恢复后自动回切/不回切的行为。
  - 重连耗尽场景：`TryReconnectNums` 很小且节点持续不可用时的返回结果。
  - 负载均衡场景：创建后连接分布是否均衡，是否会优先落到期望节点。
- 建议补测：把原来依赖 `sleep` 的 case 改成“轮询直到成功/超时失败”的风格，避免固定等待。

### `test/dbConnection_test.go`
- 问题：连接、登录、HA、重连、SQL 标准等场景都集中在一个文件，测试主题偏多。
- 问题：有 `t.SkipNow()` 的 HA case，说明该路径目前不是持续可跑状态。
- 问题：存在多处忽略错误的写法，尤其是控制节点和节点启停相关操作。
- 建议补测：
  - 连接失败、登录失败、logout/close 后再次调用的行为。
  - HA 成功切换后，`getNodeAlias()` 返回值变化是否符合预期。
  - 重连过程中的中断执行、恢复执行、重试次数耗尽。
  - `SqlStd` 不同取值下的语义差异和异常路径。
- 建议补测：给 HA 用例增加失败断言，确保“切换失败时返回什么、连接状态是什么”。

## P1: 高优先级稳定性问题

### `test/streaming/pollingClient_test.go`
- 问题：大量流订阅/取消订阅测试依赖真实流服务和共享表，环境污染风险高。
- 问题：有 `t.SkipNow()` 的 bug case，说明测试与服务版本耦合较紧。
- 问题：`time.Sleep` 较多，订阅建立、消息到达、停止发送都靠固定等待。
- 建议补测：
  - 订阅成功后能否收到首批数据、消息顺序是否正确。
  - `Reconnect=true/false` 时服务断开后的行为差异。
  - 重复订阅、重复取消订阅、`AllowExists` 的边界行为。
  - 指定非法地址、非法端口、非法表名时的错误信息是否稳定。
- 建议补测：补一组“订阅存在但消息一段时间内不来”的超时场景。

### `test/streaming/goroutineClient_test.go`
### `test/streaming/goroutineClient_reverse_test.go`
- 问题：并发测试较多，但很多依赖固定 sleep，容易有竞态和偶发超时。
- 问题：只验证最终结果的 case 多，缺少中间态断言。
- 建议补测：
  - 并发订阅/取消订阅的顺序竞争。
  - 高并发下是否有漏消息、重复消息、乱序消息。
  - 主动断开连接后，重连是否恢复订阅状态。
  - 多 goroutine 同时关闭客户端时是否幂等。
- 建议补测：增加对“取消后不再接收消息”的验证。

### `test/streaming/goroutinePooledClient_test.go`
### `test/streaming/goroutinePooledClient_reverse_test.go`
- 问题：池化客户端在并发下更容易隐藏连接复用问题，但当前更偏“跑通”验证。
- 问题：对连接池耗尽、任务排队、异常回收的覆盖不足。
- 建议补测：
  - 池大小小于并发数时的阻塞与恢复。
  - 某个 worker 异常时池是否还能继续服务。
  - 订阅和普通请求混用时的隔离性。
  - 反向场景下的消息恢复与断线重试。

## P2: 功能覆盖缺口较明显的文件

### `test/basicTypeTest/basicScalar_test.go`
### `test/basicTypeTest/basicVector_test.go`
### `test/basicTypeTest/basicMatrix_test.go`
### `test/basicTypeTest/basicTable_test.go`
### `test/basicTypeTest/basicDictionary_test.go`
### `test/basicTypeTest/basicSet_test.go`
### `test/basicTypeTest/basicPair_test.go`
### `test/basicTypeTest/basicChart_test.go`
- 问题：这组文件主要覆盖基础类型转换、上传、读取、比较，但很多场景只测单一路径。
- 问题：大量 `res, _ :=` 说明断言重点放在结果对象，不够关注错误边界。
- 建议补测：
  - 空值、全空列、混合类型、超大数值、特殊字符。
  - 上传后类型是否保持一致，`typestr()` 是否符合预期。
  - `nil`、空数组、重复值、排序/去重后的结构变化。
  - chart / dictionary / set 的边界结构和异常结构。
- 建议补测：针对每类基础类型，加一组“非法构造”和“空输入”的测试。

#### 可直接补充的场景清单

##### `test/basicTypeTest/basicScalar_test.go`
- 空标量：`NULL`、空字符串、空日期/时间类值。
- 极值标量：`int32`/`int64`/`double` 的最大最小值、`decimal` 精度边界。
- 特殊字符：中文、emoji、转义符、换行符、引号。
- 类型保持：上传后再下载，确认 `typestr()` 和原始类型一致。
- 非法输入：构造不支持的标量类型、空 `DataType`、nil 上传值。

##### `test/basicTypeTest/basicVector_test.go`
- 空向量和全空向量。
- 单元素向量、重复值向量、已排序/乱序向量。
- 混合边界：同一列含 `NULL`、极值、普通值。
- 结构一致性：上传/下载后长度、元素顺序、类型不变。
- 非法输入：空 slice、nil slice、元素类型不一致。

##### `test/basicTypeTest/basicMatrix_test.go`
- 空矩阵、1x1 矩阵、非方阵。
- 行列极小/极大边界。
- 全 0 矩阵、全空矩阵、单行/单列矩阵。
- 结构一致性：维度、行列顺序、元素类型保持。
- 非法输入：行列长度不匹配、空维度、nil 数据。

##### `test/basicTypeTest/basicTable_test.go`
- 单列/单行/全空表。
- 多列混合类型表：字符串、数值、日期、布尔、空值组合。
- 重复行、排序后表、含特殊字符字段。
- `typestr()`、列名、列数、行数的一致性。
- 非法输入：列长度不一致、列名重复、nil 列、空表定义。

##### `test/basicTypeTest/basicDictionary_test.go`
- 空 dictionary、单键值对、重复 key 覆盖行为。
- key/value 含 `NULL`、特殊字符、嵌套结构。
- key 顺序变化、取值后类型保持。
- 非法输入：key/value 长度不一致、nil key、nil value。

##### `test/basicTypeTest/basicSet_test.go`
- 空 set、单元素 set、重复元素去重行为。
- 顺序无关性验证。
- 包含 `NULL`、特殊字符、极值元素。
- 非法输入：nil 集合、类型不一致元素。

##### `test/basicTypeTest/basicPair_test.go`
- 空 pair、单 pair、左右值不同类型组合。
- 左右值含 `NULL`、特殊字符、极值。
- 嵌套 pair 的类型保持。
- 非法输入：缺失一侧、nil pair、非法组合。

##### `test/basicTypeTest/basicChart_test.go`
- 空 chart、单点 chart、多系列 chart。
- 特殊字符标签、重复标签、极值数据点。
- 正常图表和异常图表结构的一致性。
- 非法输入：缺少坐标轴、数据列不匹配、空 series。

#### 更适合统一补的公共 case
- `typestr()` 回归：每类基础类型都补一条“上传后类型字符串保持一致”的 case。
- `NULL` 回归：每类基础类型都补一条“包含空值但整体结构不变”的 case。
- 非法构造回归：每类基础类型都补一条“参数缺失 / 长度不一致 / 类型不一致”的失败 case。
- 特殊字符回归：每类字符串相关结构都补一条“中文 + 引号 + 换行”的 case。

### `test/loadTable_test.go`
### `test/loadTableBySQL_test.go`
### `test/loadText_test.go`
### `test/ploadText_test.go`
### `test/saveText_test.go`
### `test/saveTable_test.go`
- 问题：这些文件主要覆盖加载/保存链路，但依赖文件系统、表状态和远端数据库环境。
- 问题：部分 case 只验证能跑通，缺少文件不存在、权限不足、重复写入、格式不合法等路径。
- 建议补测：
  - 文件不存在、路径非法、编码异常、重复加载。
  - 保存后再加载的一致性检查。
  - SQL 加载时条件为空、条件非法、返回空集。
  - 多次保存/覆盖保存时的行为。

### `test/createDatabase_test.go`
### `test/existsDatabase_test.go`
### `test/dfsTable_test.go`
### `test/table_test.go`
### `test/undef_test.go`
- 问题：数据库、表、删除/存在性相关测试更偏操作型，缺少幂等性和异常路径。
- 建议补测：
  - 重复创建、重复删除、删除不存在对象。
  - `exists*` 的正反场景都要覆盖。
  - 表对象在不同会话/不同用户下的可见性。
  - `undef`、drop、detach 等操作后的状态检查。

### `test/newTableFromStruct_test.go`
### `test/newTableFromRawData_test.go`
### `test/multigoroutinetable_test.go`
- 问题：更偏数据构造和多协程场景，容易遗漏字段顺序、零值、异常字段映射。
- 建议补测：
  - struct 字段缺失、字段重名、匿名字段、嵌套结构。
  - raw data 中列数不一致、长度不一致、类型不匹配。
  - 多 goroutine 同时构建或上传时的线程安全。

## P3: 结构性改进建议

### `test` 目录整体
- 问题：单元测试、集成测试、环境依赖测试混在一起，命名和跳过策略不统一。
- 问题：目录中存在大量全局连接，测试之间共享资源明显。
- 问题：当前失败时的可定位性不够，很多地方忽略错误导致“假通过”。
- 建议补测：
  - 为每个测试文件补一个“失败路径”类 case，确保不是只有 happy path。
  - 为 HA / streaming / pool 类测试补统一的 setup / teardown 模式。
  - 为所有外部依赖型测试增加超时和结果校验。
  - 把长期 `SkipNow` 的 case 记录为“待恢复列表”，避免遗忘。

### 优先级建议
- P0：`setup/settings.go`、`connectionPool_test.go`、`dbConnection_test.go`
- P1：`streaming/*.go`
- P2：`basicTypeTest/*`、`load/save/*`、`database/table/*`
- P3：整体测试结构和命名规范

## 可直接执行的下一步

- 先把 `P0` 文件里的问题列成逐项任务卡，按“能否独立跑通”排序。
- 再挑 `connectionPool_test.go` 和 `dbConnection_test.go` 做第一轮稳定性改造，因为这两类最容易影响整体回归。
- 最后再补流测试和基础类型测试的边界 case。
