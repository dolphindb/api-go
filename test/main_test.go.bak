package test

import (
	"fmt"
	"os"
	"testing"
)

// 前置钩子函数，在测试之前执行
func TestMain(m *testing.M) {
	// 设置测试环境和资源
	beforeAll()

	// 运行测试
	code := m.Run()

	// 执行清理操作
	afterAll()

	// 返回测试结果
	os.Exit(code)
}

// 设置测试环境和资源
func beforeAll() {
	// 在测试之前执行的逻辑
	fmt.Print("测试前置——————————————————————————————————————————————————————————")
	// exec.Command("sh", "-c", "cd /home/server && sh stopAllnodes.sh || true && sh clear.sh && sh start.sh")

}

// 执行清理操作
func afterAll() {
	// 在测试之后执行的逻辑
	fmt.Print("测试完成——————————————————————————————————————————————————————————")
	// exec.Command("sh", "-c", "cd /home/server && sh stopAllnodes.sh || true && sh clear.sh")
}
