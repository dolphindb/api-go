# TODO

> 本清单基于对照 `../python-client-sdk` 与 `../cpp-client-sdk` 的当前实现整理。
> 涉及公开 API 命名、类型重构的项，建议评估兼容层或放在下个大版本处理。

## P0 对齐 Python/C++ 的行为与能力

- 补齐 `multigoroutinetable` 的高级能力。
  缺少 `Upsert` 模式、`modeOption`、`waitForThreadCompletion`、连接/写入回调、`enableStreamTableTimestamp`、重连参数等；Python/C++ `MultithreadedTableWriter` 已覆盖这些场景。

- 补齐流订阅高级配置与观测能力。
  Go `streaming.SubscribeRequest` 缺少 `backupSites`、`resubscribeInterval`、`subOnce`、`resubscribeTimeout`、订阅状态回调、队列深度查询等；Python/C++ 已提供。

- 评估是否补齐事件流能力。
  C++ 已有 `EventClient` ，Go 目前只覆盖基础订阅客户端。

## P1 Go 当前公开 API / 模型层设计债

- `api/client.go`：客户端接口名应从 `DolphinDB` 调整为更直接的 `Client` / `Session` 语义。
  当前文件名、构造函数和接口名三套说法混在一起。

- `api/client.go` 里的旧式封装需要系统补齐请求参数校验。
  `ExistsDatabase`、`ExistsTable`、`LoadTable`、`LoadTableBySQL`、`Table`、`TableWithCapacity`、`DropTable`、`DropPartition` 等入口会直接把请求字段拼进服务端脚本；空路径、空表名、空 SQL、列名/列类型长度不匹配等情况目前多半会变成服务端脚本错误或更晚的异常。这个面较大，建议单独按公开 API 兼容性梳理错误文案和零值语义。

- `model.DataType` / `model.DataForm` / `model.DataTypeList` 命名与职责需要整体梳理。
  `DataType` 是接口，但 `DataType() DataTypeByte` 返回的却是枚举；`DataForm` 也是同类问题；`DataTypeList` 同时承担“类型列表”和“vector 存储”职责，理解成本过高。

- `model.NewDataType` 返回 `(DataType, error)` 的接口过重。
  基础标量构造到处都要写 `if err != nil`，建议补 typed constructor / must helper / 更直白的 scalar/vector 构造入口。

- Decimal 类型表达需要重做。
  `Decimal32s` / `Decimal64s` 实际承载 `[]float64`，`writeDecimal64s` 实际写的是 `[]int64`，用户输入值和协议 raw 值被混在了一起，命名和心智模型都很混乱。

- `model/vector.go`：`Vector.Append` 只接受 `DataType`，缺少直接 append 原生标量/切片的便捷接口。
  与 Python/C++ 面向用户的自然用法差距较大。

- `streaming.SubscribeRequest` 里 `*int` / `*float32` 这类基础类型指针暴露给用户不够 Go 风格。
  同时需要明确 `Throttle` 只是本地批处理参数，不是服务端订阅参数。

- `api/pool.go`：收敛 `PoolOption` 和 `dialer.BehaviorOptions` 的重复配置。
  当前 `PoolOption` 里已有 `Timeout`、`NetTimeout`、`Reconnect`、`TryReconnectNums`、`EnableScram`、`SqlStd` 等一批和 `BehaviorOptions` 重复的字段，维护成本高，也容易再次出现透传缺漏。
  建议在 POC 结束后新增 `PoolOption.Behavior *dialer.BehaviorOptions`，并保留现有重复字段一个兼容版本：
  公开口径优先推荐新字段；旧字段标记 deprecated，但暂不删除。
  优先级建议为 `Behavior` 中显式设置的值优先，旧字段只在 `Behavior == nil` 或对应字段未设置时兜底。
  如果按兼容方式推进，可放到 `3.1.0`；真正删除旧字段再留到下个大版本。

- 清理命名与拼写问题。
  例如 `GetSubtable`、`GetSubvector`、`handlerLopper`、`patitioned_table_appender.go` 等。

- 统一日志方案，移除散落的 `fmt.Print*` 调试输出。
  已基于 Go 1.21 收敛到可注入的 `log/slog` 入口：新增 `logging.SetLogger`，库代码默认复用 `slog.Default()`，不再直接污染 stdout。
  后续剩余工作主要是继续补齐更稳定的结构化字段，例如连接地址、topic、action、节点角色等上下文 attr，而不是继续散落字符串日志。

- `streaming/topic_poller.go` 的 `Poll` 需要继续收敛到更直白的切片复制语义。
  当前 `cache` 复制实现仍有历史包袱，后续可直接改成 `slices.Clone` 并补充回归测试，避免再维护“先 `make` 再 `copy`”这类容易写错的模式。

## P1.25 可直接收敛的重复实现

- `streaming/goroutine_client.go`、`streaming/goroutine_pooled_client.go`、`streaming/polling_client.go` 需要抽共享订阅生命周期骨架。
  目前 `Subscribe` 参数校验、`reviseSubscriber`、`UnSubscribe`、`IsClosed`、`Close`、`doReconnect` 等流程基本是同构实现，只是在“消息如何消费”这一步分叉；适合把公共状态机收进基类/内部 helper，再把 handler / batch / polling 策略单独注入，避免三处一起改、三处一起漂移。

- `api/table_appender.go`、`api/patitioned_table_appender.go`、`multigoroutinetable/multi_goroutine_table.go` 需要收敛 `schema(...)` 返回值的解析逻辑。
  这几处都在重复手写从 `Dictionary` 里取 `colDefs`、`typeInt`、`partitionColumnIndex`、`partitionSchema`、`partitionType`、`partitionColumnType` 的流程；字段名、错误处理、标量/向量分支已经开始出现细节分叉，建议统一成内部 schema metadata helper，避免后续继续复制脚本和字典拆箱代码。

- `model/scalar.go`、`model/set.go`、`model/pair.go`、`model/matrix.go`、`model/table.go`、`model/dictionary.go`、`model/chart.go` 的 DataForm 样板代码可以继续压缩。
  多个类型都在重复实现 `Rows`、`GetDataForm`、`GetDataType`、`GetDataTypeString`、`GetDataFormString` 以及相近的 `String` / `Render` 外壳；可以评估把 `Category` 相关 getter 和部分格式化骨架抽到共享 helper 或嵌入层，减少“新增一个 DataForm 就复制一套样板”的模式。

- `model/parse_datatype.go` 和 `model/render_datatype.go` 的 temporal / duration / 双元素类型转换逻辑需要表驱动化。
  例如 `Date/Month/Time/Minute/Second/Datetime/DateHour/DateMinute/Timestamp/NanoTime/NanoTimestamp` 的 parse/render 函数大多只是“空值判定 + 时间单位换算”的重复模板；适合收敛成按单位和空值 sentinel 分派的 helper，降低后续修时区、边界值或 null 语义时的漏改风险。

- `model/datatype_list.go` 里同一套 datatype 分发表被复制到了多个 `switch`。
  `NewDataTypeList`、`NewEmptyDataTypeList`、`NewDataTypeListFromRawData` 以及后续 element/null/render 相关逻辑，都在重复维护“某个 `DataTypeByte` 对应哪块底层存储、null 值、构造和转换函数”；可以评估用 descriptor 表统一管理，减少新增类型或修 Decimal 语义时的多点同步成本。

- `api/log.go`、`dialer/log.go`、`streaming/log.go`、`multigoroutinetable/log.go` 的模块级 `*Log*` 包装函数可以合并。
  目前只是重复做 `fmt.Sprintf` 再转发到 `logging` 包；可以考虑让 `logging` 直接提供 component-aware 的格式化入口，或者返回带固定 `component` attr 的 logger，去掉四份几乎一致的 shim。

## P1.5 Go 生态与协程模型

> 这一组不一定要和 Python/C++ 完全对齐；相反，需要优先判断是否应该按 Go 的并发与 API 习惯重新设计。

- 继续收敛连接构造命名与 `context` 语义。
  区分 eager-connect 与 lazy-connect 的构造入口；避免继续扩散“收了 `context.Context` 但不承诺取消语义”的接口；如果确实需要取消能力，再单独设计语义明确的 `DialContext` / 操作级 context 接口。

- 长生命周期或阻塞操作应统一走 `context.Context`。
  当前 `api/client.go` 把 `ctx` 存进结构体但几乎没发挥作用，`api/pool.go`、`streaming/util.go`、`multigoroutinetable/multi_goroutine_table.go` 内部又大量直接写死 `context.TODO()`；更符合 Go 习惯的方式是“每次操作显式传 context，而不是构造时塞一个 ctx 进去”。

- 流订阅 API 应评估改成 channel/context first，而不是继续沿用 callback first 的 Python/C++ 形态。
  Go 用户通常更容易接受“返回 `<-chan Message` + `error` + `Close/Cancel`”的组合；当前 `GoroutineClient` / `GoroutinePooledClient` / `PollingClient` 三套入口把内部实现策略直接暴露给了用户。

- 公开类型名不应过度暴露实现细节。
  `GoroutineClient`、`GoroutinePooledClient`、`MultiGoroutineTable` 这些命名描述的是内部调度方式，不是用户关心的语义；更 Go 风格的命名应偏向行为和用途，而不是“底层开了几类 goroutine”。

- 并发生命周期管理应改成 `context` + `sync.WaitGroup` + 明确所有权，减少 `chan bool`、轮询和自旋式写法。
  当前存在 `IsClosed()` 轮询、`runtime.Gosched()`、空 `fmt.Print("")`、多个 goroutine 分散退出等模式，排查问题成本高，也不符合 Go 社区常见写法。

- 库内默认使用无界队列需要非常谨慎。
  `UnboundedChan` / `ringBuffer` 这类实现会让慢消费者问题变成内存问题；Go 生态里通常更强调背压、显式容量、丢弃策略或调用方可配置的缓冲策略。

- `streaming` 内部并发基元可以继续借 Go 1.21 收敛，但需要分步验证。
  `UnboundedChan` / `RingBuffer` 仍停留在“等 Go 1.18 再泛型化”的历史状态；后续可评估改成泛型版本，并同步把旧式 `sync/atomic` 函数收敛到 typed atomics，以及评估 `sync.OnceValue(s)` 在初始化路径上的使用，但这几项都涉及并发时序，建议单独做回归验证。

- 连接池异步能力应按 Go 风格设计，而不是继续沿用 taskId 轮询模型。
  当前 `api.ConnPool` 已提供“借连接/还连接/WithConn”这一层，用于显式复用同一 session；如果后续需要补“提交长任务并异步取回结果”的能力，更适合在这一层评估“阻塞 API + 用户自己起 goroutine”或者“返回 `<-chan Result` / `errgroup` 风格辅助接口”，而不是继续扩展 `addTask/isFinished/getData` 这类轮询语义。

- 公共配置和请求对象需要更 Go 风格。
  例如尽量做到零值可用，减少 `*int` / `*float32` / setter 链式调用在公开 API 中的出现；可以评估 functional options、明确的默认值规则，以及更少但更稳定的构造入口。

- 库代码应避免 `panic`、避免“打印日志后返回 nil”这类不透明失败方式。
  例如 `streaming/listening.go` 里监听失败直接 `panic`，`api/table_appender.go` 的构造函数失败时打印后返回 `nil`；这些都不利于 Go 用户按 `error` 进行恢复与组合。

## P2 流订阅并发与资源治理

- 重新梳理 subscribe / unsubscribe / reconnect / close 的状态机。
  `streaming/listening.go`、`goroutine_client.go`、`goroutine_pooled_client.go`、`polling_client.go` 当前对 goroutine、queue、conn 的所有权划分不够清晰，容易留下过期 goroutine 和后台资源消耗。

- `streaming/reconnect_item.go` 需要改成枚举加注释。
  现在 `reconnectState` 直接用 `int`，`0/1/2` 的语义不透明。

- `streaming/listening.go` 内部命名需要重写。
  `f`、`d` 这类过短变量名让取消、探测、监听三条流程很难读。

- 尽量去掉对“手动 close 队列输入端触发退出”这种隐式约定的依赖。
  `UnboundedChan` / `ringBuffer` 当前沿用了 `chanx` 风格的思路，容易误用，也不利于和 `context`、重连、优雅关闭语义对齐。

- 为流订阅补更多长时间运行的回归场景。
  包括反复订阅/退订、网络闪断、HA 切换、reverse streaming、handler 内主动退订、重复 topic/action、并发 close 等。
