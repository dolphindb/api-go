package main

import (
	"fmt"
	"os"
	"time"

	"github.com/dolphindb/api-go/v3/api"
	"github.com/dolphindb/api-go/v3/example/apis"
)

func main() {
	// 配置连接池选项
	timeout := 1 * time.Second
	opt := &api.PoolOption{
		Address:                getenv("DOLPHINDB_ADDR", apis.TestAddr),
		UserID:                 getenv("DOLPHINDB_USER", apis.User),
		Password:               getenv("DOLPHINDB_PASSWORD", apis.Password),
		PoolSize:               5,
		LoadBalance:            false,
		EnableHighAvailability: false,
		Timeout:                timeout,
		Reconnect:              true,
		TryReconnectNums:       new(int),
	}

	// 创建连接池
	pool, err := api.NewDBConnectionPool(opt)
	if err != nil {
		fmt.Printf("Failed to create connection pool: %s\n", err.Error())
		return
	}
	defer pool.Close()

	// 创建任务
	tasks := []*api.Task{
		{Script: "sleep(1000000)", Args: nil},
		{Script: "a=1+1", Args: nil},
	}

	// 执行任务
	err = pool.Execute(tasks)
	if err != nil {
		fmt.Printf("Failed to execute tasks: %s\n", err.Error())
		return
	}

	// 输出任务结果
	for _, task := range tasks {
		fmt.Println("Task succeeded:", task.IsSuccess())
		if task.GetError() != nil {
			fmt.Printf("Task failed: %s\n", task.GetError().Error())
		} else {
			fmt.Printf("Task result: %v\n", task.GetResult())
		}
	}
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}
