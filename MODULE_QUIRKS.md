# MODULE_QUIRKS

这个文档的目标不是替代代码，而是减少“每次动某个模块都要重新读完整个仓库”的上下文开销。

使用方式：
- 修改某个模块前，先看对应章节，重点关注“反常点”和“写代码建议”。
- 这里优先记录会影响实现判断的历史包袱、非直觉设计、与 Go 生态冲突的做法。
- 如果代码与文档不一致，以代码为准；修完后把文档补齐。

## `api`

热点文件：`api/client.go`、`api/pool.go`、`api/table_appender.go`、`api/patitioned_table_appender.go`

反常点：
- 客户端构造语义历史上割裂：`NewDolphinDBClient` / `dialer.NewConn` 只构造不连接，旧的 `NewSimpleDolphinDBClient` / `NewSimpleConn` 会直接连接并登录；当前新增了更直白的 `api.Dial` / `dialer.Dial` 作为 eager-connect 入口，但兼容旧名仍然存在。
- `api` 这个包名本身语义偏空，不像 Go 里常见的领域入口包；新代码优先放到 `dolphindb` 包，`api` 逐步退化为兼容层。
- `PoolOption` 里的 `LoadBalanceAddresses` 和 `HighAvailabilitySites` 历史上语义重叠；当前在 `LoadBalance=true` 时会被合并成一组候选节点，分别用于均分建连和 HA 切换。
- API 层大量通过拼接脚本字符串调用服务端，改动时要格外注意引号、句柄名、脚本注入边界和兼容性。
- `TableAppender` / `PartitionedTableAppender` 这类构造器会先拼 `schema(...)` / `schema(loadTable(...))` 拉服务端 schema；表名、分区列等必需参数应在本地先校验，避免把空参数发成服务端脚本错误。

写代码建议：
- 面向最终用户的新入口优先加到 `dolphindb` 包，并顺手把明显反 Go 的名字一起收敛，例如 `NewClient` / `CreateDatabase` / `NewTable`；不要只换包名不换语义。
- 新增入口时尽量统一“是否自动连接”的语义，不要再增加第三套行为。
- 新接口优先返回 `(..., error)`，不要沿用“打印后返回 `nil`”的模式。
- 如果只是为了满足脚本调用，不要把 `model` 层的底层协议抽象继续泄漏到更高层用户接口里。

## `dialer`

热点文件：`dialer/dialer.go`、`dialer/behavior.go`、`dialer/node.go`

反常点：
- 历史上的 stdout 日志已开始收敛到统一的 `logging` + `log/slog` 入口；SDK 默认复用 `slog.Default()`，很多 message 仍是兼容式字符串，结构化字段还需要继续补齐。
- DolphinDB server 的 raft `NotLeader` 错误可能只返回内网 `ip:port:alias`。
  启用 HA/reconnect 的连接在成功连接后会 best-effort 查询
  `select site from getClusterPerf()` 和各节点 `publicName`。后续
  `NotLeader` 目标会按当前成功连接的地址形态选择内网或 publicName 作为首选；
  首选不通时再尝试同一节点的另一种地址，发现失败必须保留原始行为。
- 请求 session id 应在真正写协议头前从当前连接读取，语义上对齐 C++ 的
  DBSession；不要在 `RunFuncWithTrace` / `Upload` 构造 `requestParams` 时
  提前缓存 session。否则请求级 failover 换到新连接后，function/variable 请求
  可能继续使用旧节点 session 并反复触发 failover。收到服务端响应头后也要刷新
  当前连接 session，即使后续响应体是服务端错误。
- 服务端可能在客户端已经切到 `NotLeader` 指定节点后继续返回同一个
  `NotLeader` target；请求级 failover 必须识别同一请求内重复 target，避免在
  `TryReconnectNums == nil` 时无限重连同一节点。

写代码建议：
- 涉及连接、重连、HA 的改动，默认视为高风险改动，先核对 Python/C++ 语义再下手。
- 尽量不要再引入新的 `context.TODO()`；如果要支持取消，优先把 `context` 贯穿到底。
- 不要继续扩大散落的 stdout 日志覆盖面；新日志统一走 `logging.SetLogger` 对应的 `log/slog` 入口。

## `model`

热点文件：`model/datatype.go`、`model/dataform.go`、`model/datatype_list.go`、`model/vector.go`、`model/io.go`

反常点：
- `DataType`、`DataForm`、`DataTypeList` 这些名字同时承担“接口”“枚举”“底层存储”的多重语义，初读非常容易混淆。
- Decimal 相关设计尤其反常：`Decimal32s` / `Decimal64s` 对用户暴露的是浮点包装，但序列化阶段写的是 raw integer。
- `Vector.Append` 只接受 `DataType`，对用户不够自然，也让很多调用点不得不先做一次中间转换。
- 这里的很多类型更像“协议中间层”，而不是最终用户最应该直接接触的模型层。

写代码建议：
- 如果不是在修协议实现，尽量不要把这些底层抽象继续向上层 API 暴露。
- 优先考虑增加高层 helper / constructor，而不是让更多调用方直接拼 `DataType` / `DataTypeList`。
- 涉及 Decimal 的改动，先分清“用户输入值”和“线协议 raw 值”，不要把两层语义混在一起。

## `streaming`

热点文件：`streaming/subscriber.go`、`streaming/listening.go`、`streaming/goroutine_client.go`、`streaming/goroutine_pooled_client.go`、`streaming/polling_client.go`、`streaming/unboundedChan.go`、`streaming/ringBuffer.go`

反常点：
- 当前对外暴露了 `GoroutineClient`、`GoroutinePooledClient`、`PollingClient` 三套客户端，名字描述的是实现手段，不是用户语义。
- callback first 的设计更像 Python/C++ 迁移过来的形态，不是特别 Go。
- goroutine 生命周期、queue 所有权、unsubscribe / reconnect / close 的交互比较脆弱。
- 使用了自维护的无界队列思路，慢消费者可能把问题放大成内存和后台 goroutine 问题。
- `SubscribeRequest` 里有不少基础类型指针字段；`Throttle` 实际是本地批处理参数，不是服务端订阅参数。
- `StreamDeserializer` 通过 `TableNames` 拼 `schema(...)` / `schema(loadTable(...))` 初始化消息 schema；新增入口或改动时要先校验 map、连接和表名，避免空表名变成服务端脚本错误。
- `listening.go` 里存在 `panic`、轮询、`runtime.Gosched()`、空打印等非常规库代码写法。

写代码建议：
- 新设计优先考虑 channel/context first，而不是继续加新的 callback 分支。
- 涉及 goroutine 退出、重连、关闭流程时，先想清楚 owner、退出信号和资源回收，再写代码。
- 默认避免无界缓存；如果必须缓冲，至少让容量和策略可见、可控。
- 改 streaming 时顺手检查有没有机会把“实现细节命名”收敛成“行为语义命名”。

## `multigoroutinetable`

热点文件：`multigoroutinetable/multi_goroutine_table.go`、`multigoroutinetable/writer_goroutine.go`、`multigoroutinetable/status.go`

反常点：
- 整体思路继承自 Python/C++ 的 `MultithreadedTableWriter`，但能力只覆盖了一部分。
- 包名和类型名都暴露了底层 goroutine 实现细节。
- 内部会自己创建多条连接和多个 goroutine，并且大量直接使用 `context.TODO()`。
- 公开状态接口、未写入数据接口已经被 README 和现有用法依赖，动行为时要非常小心。
- 构造阶段会拼 `schema(...)` / `schema(loadTable(...))` 和 writer 里的 `tableInsert{...}`；`TableName` 这类必需参数要在连接服务端前本地校验，避免空值一路传到脚本层。

写代码建议：
- 如果要继续扩展这个模块，优先考虑“Go 风格并发写入器”而不是机械补齐所有 Python/C++ 形态。
- 改动 writer 生命周期、close、重试、未写入数据语义时，要一起看 `README.md` 和现有开发测试。
- 能通过更清晰的 owner / context / wait 语义替代隐式 goroutine 管理的，优先替代。

## `domain` / `errors`

整体相对简单，更多是支撑模块。

写代码建议：
- 这两个包优先保持小而稳，避免把其他模块的历史包袱继续搬进来。

## 跨模块共识

- 优先返回 `error`，避免 `panic`，避免“打印日志后返回 `nil`”。
- 优先让 `context.Context` 出现在真正的阻塞/长生命周期操作上，而不是只在构造函数里传一下。
- 优先用行为语义命名，而不是用“底层开了多少 goroutine/线程”来命名公开类型。
- 如果 Go 生态习惯和 Python/C++ 现状冲突，优先考虑 Go 习惯，但要把偏离原因写进文档。
- 每次确认一个新的“反常点”，记得更新这个文件，避免下次再付一次上下文成本。
