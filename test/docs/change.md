1,
旧写法：newtable := model.NewTable(...)
新写法：newtable, err := model.NewTable(...)

2,NewTableAppender：从“单返回值”升级成了“对象 + error”

3,BehaviorOptions 的 SqlStd 改为直接赋字段：&dialer.BehaviorOptions{SqlStd: tc.SqlStd}

4,NewDictionary / NewChart：从“单返回值”升级成了“对象 + error”，相关测试已同步改为接收 err 并断言 ShouldBeNil
5,NewMatrix：从“单返回值”升级成了“对象 + error”，相关测试已同步改为接收 err 并断言 ShouldBeNil
6,NewSet：从“单返回值”升级成了“对象 + error”，相关测试已同步改为接收 err 并断言 ShouldBeNil
7,NewPair：从“单返回值”升级成了“对象 + error”，相关测试已同步改为接收 err 并断言 ShouldBeNil
8,NewVectorWithArrayVector：从“单返回值”升级成了“对象 + error”，相关测试已同步改为接收 err 并断言 ShouldBeNil
9, basicPair_test.go 中 db.Upload 相关的 err 复用写法已统一修正为 `_, err = ...`，避免 `:=` 重复声明报错
10, basicSet_test.go 中 db.Upload / db.RunScript 相关的 err 复用写法已统一修正为 `_, err = ...`，避免 `:=` 重复声明报错
11, basicVector_test.go / basicDictionary_test.go / basicScalar_test.go / basicTable_test.go 中继续统一修正了 err 复用写法，所有 `_, err := db.Upload(...)` 和相关 `NewTableFromRawData` 调用已改为 `=`
12, basicTable_test.go 中 `Test_table_boundary_mismatched_input` 需要重新声明局部 err，已改回 `_, err := model.NewTableFromRawData(...)` 以避免 `undefined: err`
13, test/streaming/util.go 已同步修正 `NewVectorWithArrayVector` 的双返回值签名
14, test/streaming 目录下 pollingClient / pollingClient_reverse / goroutineClient_reverse / goroutinePooledClient_reverse 等测试继续同步修正 `NewVectorWithArrayVector` 的双返回值签名，以及 `appender.Append` 的 `err` 复用写法
