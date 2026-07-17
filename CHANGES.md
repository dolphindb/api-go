# CHANGES

## `3.2.0`

- 升级依赖版本，修复安全漏洞 CVE-2026-46595

## `3.1.0`

### 接口优化
- 新增 `dolphindb` 高层入口包，作为面向最终用户的首选命名空间。新包提供更贴近 Go 习惯的入口名：`dolphindb.NewClient`、`dolphindb.Dial`、`(*Client).CreateDatabase`、`(*Client).NewTable`、`dolphindb.NewTaskPool`、`dolphindb.NewConnPool`。
- 新增 `dolphindb.Dial` 和 `dialer.Dial`，用于“一步完成连接并登录”的更直接入口；旧的 `NewSimple*` 继续保留兼容，但已不再推荐。

### 功能增强
- 新增统一日志入口 `logging.SetLogger`，库内日志收敛到 Go 1.21 `log/slog`；默认复用 `slog.Default()`，也可通过 `logging.SetLogger` 替换。
- `TryReconnectNums` 语义调整为：`nil` 表示无限重试，正整数表示有限重试次数，小于等于 `0` 的值会返回参数错误。

### 完善错误检查
- 下列公开构造/初始化接口现在统一返回 `error`，不再通过返回 `nil`、打印日志或延后到序列化阶段才暴露失败：`api.NewTableAppender`、`model.NewTable`、`model.NewPair`、`model.NewSet`、`model.NewMatrix`、`model.NewDictionary`、`model.NewChart`、`model.NewVectorWithArrayVector`。
- `model.NewTableFromRawData` 和 `model.NewTable` 现在会显式检查各列行数是否一致；`model.Table.Render` 在真正写请求前也会再次校验，避免构造出坏表后到服务端通信阶段才表现为 EOF、断连接或其他非直观错误。
- `api.NewTableAppender` 现在会对 `nil` option / `nil` connection 显式返回错误；取表 schema 或解析 `colDefs` 失败时也会直接返回 `error`，不再只打印日志后返回 `nil`。
- `model.NewPair` 现在会检查输入 `Vector` 非空且长度必须为 `2`；`model.NewSet`、`model.NewMatrix`、`model.NewDictionary` 分别补充了空参数和键值行数不一致等本地校验。
- `model.NewChart` 现在会校验支持的字段名及字段类型，错误输入会直接返回 `error`，不再依赖运行时类型断言 panic。
- `model.NewVectorWithArrayVector` 现在会对空 array vector 和不支持的 array vector 类型直接返回 `error`。
- `multigoroutinetable` 的批量写入路径增加了本地预校验：批量列类型不匹配、空批次、批量列长度不一致、非法 array vector 输入等情况都会在客户端直接返回明确错误，不再静默漏写后拖到网络通信阶段暴露。
- `DBConnectionPool` 的关闭态和借还连接行为更明确：连接池关闭后会返回清晰错误，避免隐式失败。
- `streaming` 订阅初始化在监听端口不可用时现在会直接返回 `error`；之前这类场景会在后台 goroutine 里触发 `panic`。
- `streaming` 的 `TopicPoller.Poll` 在 `MsgAsTable=true` 且本次轮询未取到消息时，现返回空切片；之前会在内部合表阶段因空消息切片访问 `msg[0]` 触发 panic。
- `streaming` 的 topic 解析增加了格式校验；对缺少 `"/"` 分隔符的异常 topic 现在会忽略并记录日志，而不是在重连状态维护或站点枚举时因为字符串切片越界 panic。
- `streaming` 的消息解析增加了空 schema table、空 vector 和非法首元素检查，异常流消息现在会被忽略并记录日志，避免在取第 1 列或首元素时触发 panic。
- `streaming` 内部 `RingBuffer` 的空队列 `Peek`/`Pop` 和非正初始容量现改为安全行为，不再因为误用直接 panic。
- `dialer/protocol` 的零拷贝切片/字符串转换补充了空输入保护，避免空字节切片经过 `&b[0]` 转换时触发 panic。
