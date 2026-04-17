package main

import (
	"fmt"
	"time"

	"github.com/dolphindb/api-go/v3/api"
	"github.com/dolphindb/api-go/v3/example/apis"
)

func main() {
	// 配置连接池选项
	opt := &api.PoolOption{
		Address:  apis.TestAddr,
		UserID:   apis.User,
		Password: apis.Password,
		PoolSize: 5,
		// 开启后若同时配置 LoadBalanceAddresses，
		// 连接会在 Address、LoadBalanceAddresses、HighAvailabilitySites
		// 去重后平均分配。
		LoadBalance:            false,
		EnableHighAvailability: false,
		Reconnect:              true,
		TryReconnectNums:       new(int),
	}

	fmt.Printf(
		"Connecting to %s with request Timeout=%s and NetTimeout=%s\n",
		opt.Address,
		describeTimeout(opt.Timeout, time.Minute),
		describeTimeout(opt.NetTimeout, 3*time.Second),
	)

	// 创建连接池
	pool, err := api.NewDBConnectionPool(opt)
	if err != nil {
		fmt.Printf("Failed to create connection pool: %s\n", err.Error())
		return
	}
	defer pool.Close()

	// 创建任务
	singleTask := &api.Task{Script: "1..10"}
	err = pool.ExecuteTask(singleTask)
	if err != nil {
		fmt.Printf("Failed to execute task: %s\n", err.Error())
		return
	}
	fmt.Printf("Single task result: %v\n", singleTask.GetResult())

	// 创建批量任务
	tasks := []*api.Task{
		{Script: "sleep(100)", Args: nil},
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

func describeTimeout(value, defaultValue time.Duration) string {
	if value == 0 {
		return fmt.Sprintf("default(%s)", defaultValue)
	}

	return value.String()
}
