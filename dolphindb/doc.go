// Package dolphindb provides the preferred high-level client API for DolphinDB.
//
// This package is the recommended entry point for application code. It keeps the
// core session, pooling, and table appender APIs under a Go-friendly namespace
// while reusing the lower-level api, dialer, and model packages for request
// options and DolphinDB data values.
//
// Use Dial when you want to create a session, connect, and log in in one step:
//
//	client, err := dolphindb.Dial("127.0.0.1:8848", "admin", "123456", nil)
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//
//	result, err := client.RunScript("1 + 1")
//	if err != nil {
//		return err
//	}
//	_ = result
//
// Use NewClient when connection setup needs to be staged or when you want to
// configure the client before logging in:
//
//	client, err := dolphindb.NewClient(ctx, "127.0.0.1:8848", nil)
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//
//	if err := client.Connect(); err != nil {
//		return err
//	}
//	err = client.Login(&dolphindb.LoginRequest{
//		UserID:   "admin",
//		Password: "123456",
//	})
//
// Client exposes the common DolphinDB session operations, including RunScript,
// RunFile, RunFunc, Upload, Login, Logout, Close, and connection state checks.
// For pooled usage, NewConnPool creates an explicit borrow/return pool and
// NewTaskPool creates the task-oriented pooled executor. For table writes, use
// NewTableAppender or NewPartitionedTableAppender.
//
// The model package remains the place to construct DolphinDB values such as
// scalars, vectors, tables, dictionaries, and matrices. The dialer package
// provides connection behavior options such as load balancing, high
// availability, priority, parallelism, and fetch size. The api package remains
// available for lower-level compatibility APIs that have not been lifted into
// this package.
//
// See the repository README and the example directory for complete runnable
// programs.
package dolphindb
