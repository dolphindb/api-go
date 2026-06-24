# MODULE_DESIGN

这个文档记录当前认可的模块设计和实现边界。它和 `MODULE_QUIRKS.md`
分工不同：这里放主动设计决策，`MODULE_QUIRKS.md` 放历史包袱、反常点和容易踩坑的约束。

## `api` / `dolphindb` pooling

- `api.DBConnectionPool` 是 task-oriented 的批量执行器，负责 `Execute` /
  `ExecuteTask` 这类任务队列语义。
- 显式借还连接使用 `dolphindb.ConnPool`。它和 `DBConnectionPool` 分开实现，
  避免把“用户持有 session”与“任务队列内部调度连接”混在一起。
- `dolphindb.ConnPool` 通过 `ConnLease` / `WithConn` 管理归还；裸
  `Release(conn)` 只是低级接口。

## `DBConnectionPool` raft leader handling

- `dialer.Conn` 的 `RunScriptWithTrace` / `RunFuncWithTrace` 返回
  `dialer.ExecutionTrace`，用于描述本次请求执行过程中发生的 failover。
- `dialer.Conn` 在启用 HA/reconnect 时会尽量维护一份集群内网地址和
  `publicName` 地址的对应关系。`NotLeader` 指向内网地址时，连接会根据
  当前成功连接使用的是内网地址还是 publicName 来选择首选目标；首选不通时
  再尝试同一节点的另一种地址。trace 的 `Reason` 描述客户端本次移动的语义：
  `NotLeader` 表示 server-directed 目标，`ServerDirectedFallback` 表示同一节点
  地址形态 fallback，`HighAvailability` 表示进入 HA 候选节点轮询。
- `DBConnectionPool` 会读取 trace 里的 failover target。任务失败时只使用
  `NotLeader` 指定的新 leader 做一次应用层重试和最终整池切换；
  任务已经在 conn-level failover 后成功时，只有 trace 中出现过
  `NotLeader`，任务池才会把内部连接整体重建到最后实际连上的 target。
  纯 HA failover 成功只影响当前连接，不作为 leader 切换维护。
- 内部连接使用 generation 标识所属批次。切换 leader 时 generation 递增，
  新建一组连接并替换当前 channel；旧 channel 中的空闲连接会被关闭。
- 在途旧连接归还时，如果 generation 已过期，直接关闭，不会混回新池。
- 如果任务已经成功，leader 切换只是提前维护任务池状态；切换失败不影响该任务
  返回成功，后续任务仍可再次触发切换。
- 如果任务因 `NotLeader` 失败，任务池会先用当前 borrowed connection 重试该任务
  一次，再根据首次执行和重试 trace 得到的最终 target 做一次整池切换；避免先切到
  中间 leader、重试后又切到最终 leader。如果切换失败，则保留原任务错误，不把
  维护性切换错误冒充成任务执行错误。
