# MODULE_QUIRKS

这个文档的目标不是替代代码，而是减少“每次动某个模块都要重新读完整个仓库”的上下文开销。

使用方式：
- 修改某个模块前，先看对应章节，重点关注“反常点”和“写代码建议”。
- 这里优先记录会影响实现判断的历史包袱、非直觉设计、与 Go 生态冲突的做法。
- 如果代码与文档不一致，以代码为准；修完后把文档补齐。

## `api`

热点文件：`api/client.go`、`api/pool.go`、`api/table_appender.go`、`api/patitioned_table_appender.go`

反常点：
- 客户端构造语义割裂：`NewDolphinDBClient` / `dialer.NewConn` 只构造不连接，`NewSimpleDolphinDBClient` / `NewSimpleConn` 会直接连接并登录。
- `PoolOption.Timeout` 目前存在但没有完整透传到连接创建流程。
- `NewTableAppender` 不是典型 Go 风格：失败时会打印日志并返回 `nil`，而不是显式返回 `error`。
- API 层大量通过拼接脚本字符串调用服务端，改动时要格外注意引号、句柄名、脚本注入边界和兼容性。

写代码建议：
- 新增入口时尽量统一“是否自动连接”的语义，不要再增加第三套行为。
- 新接口优先返回 `(..., error)`，不要沿用“打印后返回 `nil`”的模式。
- 如果只是为了满足脚本调用，不要把 `model` 层的底层协议抽象继续泄漏到更高层用户接口里。

## `dialer`

热点文件：`dialer/dialer.go`、`dialer/behavior.go`、`dialer/node.go`

反常点：
- `NewConn` 和 `NewSimpleConn` 的行为差异很大，但命名不够直观。
- HA / reconnect / `TryReconnectNums` 相关语义历史上一直容易出错，改动时需要同时看 `TODO.md`。
- 对外暴露了 `context.Context`，但内部不少路径并没有真正贯彻取消语义。
- 日志大量散落在 `fmt.Print*`，调试输出和正式行为混在一起。

写代码建议：
- 涉及连接、重连、HA 的改动，默认视为高风险改动，先核对 Python/C++ 语义再下手。
- 尽量不要再引入新的 `context.TODO()`；如果要支持取消，优先把 `context` 贯穿到底。
- 不要继续扩大 stdout 日志覆盖面，优先考虑统一 logger 或至少集中封装。

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
