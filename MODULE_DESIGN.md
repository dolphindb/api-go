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
- 非负载均衡池构造时把 `currentLeader` 初始化为 `PoolOption.Address`。
  `LeaderConvergenceWait(A→A)` 表示连接仍在原节点等待，不是 leader 切换，
  不得触发 generation 递增或整池重建。负载均衡池初始连接可能分布在多个节点，
  因而不声明单一的初始 `currentLeader`。
- 内部连接使用 generation 标识所属批次。切换 leader 时 generation 递增，
  新建一组连接并替换当前 channel；旧 channel 中的空闲连接会被关闭。
- 在途旧连接归还时，如果 generation 已过期，直接关闭，不会混回新池。
- 如果任务已经成功，leader 切换只是提前维护任务池状态；切换失败不影响该任务
  返回成功，后续任务仍可再次触发切换。
- 如果任务因 `NotLeader` 失败，任务池会先用当前 borrowed connection 重试该任务
  一次，再根据首次执行和重试 trace 得到的最终 target 做一次整池切换；避免先切到
  中间 leader、重试后又切到最终 leader。如果切换失败，则保留原任务错误，不把
  维护性切换错误冒充成任务执行错误。

## 请求级 leader convergence

- 结构化 `NotLeader` 是服务端在执行当前操作前返回的 server-directed 拒绝。
  `dialer.conn.runWithTrace` 只对这一类错误扩大请求内重放；`EOF`、read timeout、
  connection reset 等执行结果不确定的 I/O 错误不进入该规则。
- 第一次收到 target 时保持 server-directed failover；同一规范化 target 再次出现时，
  最多执行 90 次收敛重试，并受 `BehaviorOptions.LeaderConvergenceTimeout` 的时间
  预算约束。默认预算 60 秒；次数和时间任一先耗尽就返回最后的结构化错误。90 次是
  防御性第二上限，避免 ±20% jitter 较小时在 60 秒之前先耗尽次数，正常情况下由
  时间预算先行兜底。
- 收敛预算从当前请求第一次收到结构化 `NotLeader` 时开始，不是整个
  `RunScript` / `RunFunc` 的总 deadline。此前 `UnknownLeader`、
  `DataNodeNotAvail` 和无 target HA 轮询消耗的时间不计入；当
  `TryReconnectNums=nil` 时，这些既有无 target 路径仍可能无界等待。
- 内网地址和 `publicName` 先规范化到同一节点再计数，不能靠地址形态切换绕开预算。
  退避为 300ms、600ms、1s 封顶，并加 ±20% jitter。
- target 等于当前逻辑连接节点时不重新建连，保留 session 并原地重放；异址时仍可
  在预算内切向 target。`TryReconnectNums` 非空时继续作为既有外层请求尝试上限，
  leader convergence 不放大用户配置。
- 每次收敛迭代只追加一条 trace：实际跨节点移动使用 `NotLeader`，同址保留
  session 并等待重放使用 `LeaderConvergenceWait`。预算耗尽时追加最后一条 wait
  事件并保留原始结构化错误，不记录并未发生的节点移动。连接池失败路径仍只从最后
  一个 `NotLeader` 提取 server-directed target；只有 wait 的同址成功请求不会触发
  整池切换。
- `FailoverTrace.From` 表示发起本次操作时的逻辑连接地址，优先使用
  `connectedAddress`，不再依赖 DNS 解析后的 `RemoteAddr` 或 node pool 的上次索引。
- 若下一次退避长于剩余收敛预算，等待会截断到 remaining 后再做最后一次重放，
  不会因为完整退避放不下而提前放弃。
- `RunScript` 可以包含多语句。理论上后续语句返回 `NotLeader` 时，前序语句可能已经
  提交；SDK 原本就会重放一次，本设计扩大的是次数而不是风险类别。需要严格
  exactly-once 的多语句写入仍应使用业务批次 ID、幂等键或服务端去重。
- 切换 TCP 连接时，先完成新连接拨号和 socket option 配置，再关闭旧 socket 并安装
  新 reader/connection；握手失败会关闭新 socket并清理连接状态。关闭旧 socket
  失败只记录 warning，不丢弃已建立的新 TCP。
