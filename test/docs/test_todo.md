# TODO

## 一、补充测试场景

### 1. 核心 API 覆盖缺口

| 功能 | 优先级 | 说明 |
|------|--------|------|
| `RunFunc` 集成测试 | P0 | 当前 `test/` 中无任何测试覆盖 `RunFunc`。需覆盖函数名+参数的各种数据类型组合、空参数、错误函数名等 |
| `RunFile` 集成测试 | P1 | 从文件加载脚本执行，当前无测试覆盖 |
| `Upload` 变量上传测试 | P1 | 通过 `conn.Upload()` 上传多个本地变量到服务端的场景 |
| `Login` / `Logout` | P0 | `AccountAPI.Login/Logout` 无集成测试。需覆盖：登录成功、失败、重复登录、登录后操作权限变化 |
| `UndefAll` | P1 | 当前只有 `Undef` 单个删除的测试，缺少全量 undef 的验证 |
| `ClearAllCache` | P1 | 当前无集成测试 |

### 2. TableAppender / PartitionedTableAppender

| 功能 | 优先级 | 说明 |
|------|--------|------|
| `TableAppender` 集成测试 | P0 | 无任何集成测试。需覆盖：内存表/DFS 表追加、各种数据类型追加、空表追加、错误表名/路径 |
| `PartitionedTableAppender` 集成测试 | P1 | 需覆盖：正常追加、并发追加、分区列指定、错误分区名 |

### 3. 连接池 (DBConnectionPool)

| 场景 | 优先级 | 说明 |
|------|--------|------|
| `Execute` 批量任务 | P1 | 批量提交脚本并等待所有结果返回 |
| `Acquire` / `Release` 循环 | P1 | 反复借用、归还连接的稳定性测试 |
| `RunTask` 在指定连接上执行 | P1 | 特定连接上执行特定任务 |
| `RefreshTimeout` 后继续使用 | P2 | 修改超时后连接池行为验证 |
| `NewDolphinDBClient` + BehaviorOptions | P1 | 当前测试只覆盖 `NewSimpleDolphinDBClient`，缺少对 `NewDolphinDBClient` + `BehaviorOptions` 组合的全面测试 |

### 4. 数据类型边界场景

| 场景 | 优先级 | 说明 |
|------|--------|------|
| Decimal32/Decimal64 精度与边界 | P1 | 大精度小数、负值、零、最大值/最小值 |
| Blob 大数据量 | P1 | 大 blob(>1MB) 写入与读取、UTF8/二进制混合 |
| int128 / ipaddr 写入与读取 | P1 | 当前测试以 RunScript 方式执行，缺少 Upload 后读取 |
| NULL 语义全覆盖 | P1 | 对每种数据类型验证 NULL 写入→读取的一致性，尤其是数值型 NULL 的零值与 nil 区分 |
| arrayVector 复杂嵌套 | P2 | 多层数组向量、空子向量、不同类型组合 |
| Symbol 类型特殊行为 | P2 | 大量重复 symbol 时的性能与压缩行为 |

### 5. 错误与异常处理

| 场景 | 优先级 | 说明 |
|------|--------|------|
| 服务端错误码解析 | P0 | NotLeader / UnknownLeader / DataNodeNotAvail 异常时的 Failover 行为测试 |
| 连接断线重连 | P0 | 网络闪断、服务端重启过程中的自动重连行为 |
| SCRAM 认证失败 | P1 | 错误密码重试、SCRAM 与非 SCRAM 混合场景 |
| 空路径/空表名的错误信息 | P1 | 验证所有 API 在空参数时返回明确错误而非 panic 或服务端脚本错误 |
| 并发关闭连接 | P1 | 多 goroutine 并发 `Close()` 不应 panic |
| 订阅重复 topic/action | P1 | 流订阅的重复订阅行为测试 |
| 超时场景 | P2 | `NetTimeout` 触发后的连接恢复 |

### 6. 流订阅 (Streaming)

| 场景 | 优先级 | 说明 |
|------|--------|------|
| 长时运行稳定性 | P1 | 持续数小时的订阅/消费测试 |
| 反复订阅/退订 | P1 | 同一 topic 反复 subscribe/unsubscribe 100+ 轮次 |
| reverse streaming 完整覆盖 | P1 | 当前 reverse 测试缺少 handler 异常恢复场景 |
| backupSites 配置 | P1 | 主节点不可用时自动切换到备份站点 |
| `subOnce` 语义 | P2 | 一次性订阅（非持久化）的场景 |
| 订阅状态回调 | P2 | 连接中断、重连、数据丢失等状态通知 |

### 7. 并发与资源管理

| 场景 | 优先级 | 说明 |
|------|--------|------|
| 多客户端并行读写 | P1 | 10+ 个并发连接同时对同一数据库进行操作 |
| 连接池并发获取 | P1 | 大量 goroutine 同时 `Acquire` 和 `Release` 连接 |
| connection 泄漏检测 | P2 | 测试 `Close()` 后池中连接是否全部释放 |

### 8. MultiGoroutineTable

| 场景 | 优先级 | 说明 |
|------|--------|------|
| 基础写入 | P0 | 无集成测试。需覆盖正常写入、多类型列写入 |
| Upsert 模式 | P1 | upsert 写入的覆盖验证 |
| 自定义 `modeOption` | P1 | 不同写入模式的行为差异 |
| 连接/写入回调 | P2 | 回调函数的触发验证 |
| 重连参数 | P1 | 写入线程在网络中断后的自动恢复 |

---

## 二、当前代码问题

### P0 - 编译/语法问题

| # | 文件 | 行号 | 问题 | 严重度 |
|---|------|------|------|--------|
| 1 | `dialer/server_error.go` | 39 | `func (e *ServerError) Is(code ServerErrorCode) bool` 签名应为 `Is(error) bool` 以正确实现 `errors.Is` 接口。当前签名导致 `errors.Is(err, ServerErrNotLeader)` 匹配逻辑不符合标准 Go 惯例 | H |
| 2 | `example/client/main.go` | 40 | `client.NewTable` 方法不存在。该示例自 `TableRequest` API 变更后未更新，直接导致 `go build` 失败 | H |
| 3 | `dialer/protocol/unsafeslice.go` | 27,30,36 | `reflect.SliceHeader` 直接使用将在 Go 1.21+ 被 `unsafe.Pointer` 替代方案淘汰，`go vet` 会警告 `possible misuse of reflect.SliceHeader` | M |
| 4 | `dialer/protocol/unsafeslice.go` | 185 | `reflect.StringHeader` 同样已被废弃，`go vet` 警告 | M |
| 5 | `test/streaming/reverse/*` | 多处 | `helper.Tuple` 结构体字面量大量使用无 key 字段初始化，结构体字段顺序变更时静默错误，`go vet` 警告 | L |

### P0 - 逻辑与资源问题

| # | 文件 | 行号 | 问题 | 严重度 |
|---|------|------|------|--------|
| 6 | `dialer/response.go` | 45-57 | MSG 循环缺少防护：如果服务端持续发送 MSG 但不含 `\0`，`ReadBytes(StringSep)` 会一直阻塞直到超时；缺少最大消息数限制可导致无限循环 | H |
| 7 | `api/client.go` | 多处 | `context.Context` 被存入结构体但未实际用于取消/超时。`NewSimpleDolphinDBClient` 的 `ctx` 参数实际是空摆设；所有操作硬编码 `context.TODO()` | H |
| 8 | `streaming/listening.go` | 监听失败处 | 端口监听失败直接 `panic`，不符合库代码惯例。应返回 `error` 让调用方决定恢复策略 | H |
| 9 | `api/table_appender.go` | 构造函数 | 连接失败写入日志后返回 `nil, nil`，调用方对 nil 进行方法调用会 panic | H |
| 10 | `dialer/dialer.go` | `run` 方法 | `run` 内部调用 `parseResponse` 后不检查 `err` 就直接使用 `*responseHeader`，可能 nil 解引用 | M |
| 11 | `dialer/response.go` | 66 | `strconv.Atoi` 的 `error` 被忽略 (`h.objectCount, _ = ...`)，服务端返回损坏数据时 objectCount 为 0 且不报错 | M |
| 12 | `test/run_function_test.go` | 147 | `ddb.RunScript("print(...)")` 的返回值和 error 被完全丢弃，print 失败时无感知 | M |
| 13 | `test/run_function_test.go` | 116 | `t.Parallel()` 与全局 `os.Stdout` 重写共存，其他并行测试的输出会污染该测试的断言 | M |

### P1 - 架构与设计问题

| # | 文件 | 问题 | 说明 |
|---|------|------|------|
| 14 | `api/client.go` | `DolphinDB` 接口名过于泛化 | 一个 SDk 中 `DolphinDB` 是接口名而不是 `Client`，与 Python/C++ SDK 命名不一致 |
| 15 | `api/client.go` | 接口方法中大量 `Deprecated` | `LoadTable`, `SaveTable`, `Table`, `TableWithCapacity`, `Database`, `DropDatabase`, `DropTable`, `DropPartition` 均被标注 deprecated 但未提供清晰的迁移路径 |
| 16 | `dialer/dialer.go` | 连接重连逻辑依赖外部轮询 | `doReconnect` 在 `streaming` 中三处重复实现，未收敛到 dialer 层统一管理 |
| 17 | `streaming/*` | 公共状态机重复 | `GoroutineClient`、`GoroutinePooledClient`、`PollingClient` 的 `Subscribe`/`UnSubscribe`/`doReconnect`/`Close` 逻辑基本同构，三处维护，一处漂移三处受影响 |
| 18 | `streaming/reconnect_item.go` | reconnection 状态用 `int` 表示 | `const ( reconnectStopped=0, reconnecting=1, reconnected=2 )` 无类型安全，应使用 `iota` 枚举类型 |
| 19 | `model/datatype_list.go` | `DataTypeList` 职责过重 | 同时承载"类型列表"和"vector 存储"职责，命名与实际用途不一致 |
| 20 | `model/vector.go` | `Vector.Append` 只接受 `DataType` | 缺少直接 append 原生 Go 标量/切片的便捷接口，用户使用体验差 |

### P1 - 依赖与兼容性问题

| # | 文件 | 问题 | 说明 |
|---|------|------|------|
| 21 | `api/pool.go` | `PoolOption` 与 `dialer.BehaviorOptions` 重复配置 | Timeout/NetTimeout/Reconnect/TryReconnectNums/EnableScram/SqlStd 等字段在两处重复定义，维护成本高 |
| 22 | 多处 | 日志输出不一致 | `api/log.go`、`dialer/log.go`、`streaming/log.go`、`multigoroutinetable/log.go` 四处日志 shim 重复实现，应合并 |
| 23 | `model/parse_datatype.go` etc. | Temporal 类型解析/渲染表驱动化 | Date/Month/Time/Minute/Second/Datetime 等的 parse/render 是重复模板，容易漏修时区或 null 语义 |
| 24 | `api/table_appender.go` etc. | `schema(...)` 解析在多处重复 | `table_appender.go`、`patitioned_table_appender.go`、`multi_goroutine_table.go` 都在重复实现从 Dictionary 解析 colDefs/typeInt 的流程 |

### P2 - 代码风格与可维护性

| # | 文件 | 问题 | 说明 |
|---|------|------|------|
| 25 | `api/patitioned_table_appender.go` | 文件名拼写错误 | `patitioned` → `partitioned` |
| 26 | `model/symbol_base_collecttion.go` | 文件名拼写错误 | `collecttion` → `collection` |
| 27 | 流订阅多处 | 短变量名可读性差 | `f`、`d`、`ch` 等含义不明的命名 |
| 28 | `streaming/topic_poller.go` | `Poll` 复制实现老旧 | `make` + `copy` 模式可改用 `slices.Clone` |
| 29 | `dialer/response.go` | `fmt.Print` 在库代码中使用 | 已部分修复但在其他地方仍可能存在 lib 代码直接写 stdout 的情况 |

---

## 三、`go vet` 检测结果汇总

```
dialer/protocol/unsafeslice.go:27,30,36    reflect.SliceHeader misuse (unsafe)
dialer/protocol/unsafeslice.go:185         reflect.StringHeader misuse (unsafe)
dialer/server_error.go:39                  Is(code) bool 应签名 Is(error) bool
streaming/subscriber_test.go:50,52,69      拷贝锁值 (sync.Map contains sync.Mutex)
test/streaming/reverse/*_test.go            Tuple 结构体无 key 字段初始化 (大量)
example/client/main.go:40                  client.NewTable 不存在
```
