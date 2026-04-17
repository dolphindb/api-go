package dialer

import "time"

// SqlStandard specifies which SQL standard should be used for the session.
// The numeric values must stay compatible with the existing DolphinDB protocol:
// DolphinDB=0, Oracle=1, MySQL=2.
type SqlStdEnum int

const (
	// SqlStdDolphinDB uses DolphinDB SQL semantics.
	SqlStdDolphinDB SqlStdEnum = 0
	// SqlStdOracle uses Oracle-compatible SQL semantics.
	SqlStdOracle SqlStdEnum = 1
	// SqlStdMySQL uses MySQL-compatible SQL semantics.
	SqlStdMySQL SqlStdEnum = 2
	// SqlStdDefault keeps the default DolphinDB SQL semantics.
	SqlStdDefault = SqlStdDolphinDB
)

func (s SqlStdEnum) String() string {
	switch s {
	case SqlStdDolphinDB:
		return "DolphinDB"
	case SqlStdOracle:
		return "Oracle"
	case SqlStdMySQL:
		return "MySQL"
	default:
		return "Unknown"
	}
}

// BehaviorOptions helps you configure behavior identity.
// Refer to https://github.com/dolphindb/Tutorials_CN/blob/master/api_protocol.md#254-%E8%A1%8C%E4%B8%BA%E6%A0%87%E8%AF%86 for more details.
type BehaviorOptions struct {
	// Priority specifies the priority of the task
	Priority *int
	// Parallelism specifies the parallelism of the task
	Parallelism *int
	// FetchSize specifies the fetchSize of the task
	FetchSize *int
	// Timeout specifies how long a request waits for the server response.
	Timeout time.Duration
	// NetTimeout specifies the network-layer timeout budget used for TCP
	// connect, Linux TCP_USER_TIMEOUT and keepalive probing. Zero uses the SDK
	// defaults.
	NetTimeout time.Duration

	// Whether to enable load balancing.
	// If true, connect to the address with the fewest connections.
	LoadBalance bool

	// Whether to enable high availability.
	// If true, the connection tries addr first, then retries HighAvailabilitySites in a shuffled order.
	EnableHighAvailability bool

	// Available only when EnableHighAvailability is true.
	HighAvailabilitySites []string

	// If true, the address will be reconnected util the server is ready.
	Reconnect bool

	// IsReverseStreaming specifies whether the job is a reverse stream subscription
	IsReverseStreaming bool
	// IsClearSessionMemory specifies whether to clear session memory after the job
	IsClearSessionMemory bool

	// tryReconnectNums specifies the number of times to try reconnecting
	TryReconnectNums *int

	// UsePython specifies whether the session uses a Python parser
	UsePython bool

	// SqlStd specifies which SQL standard should be used for the session.
	SqlStd SqlStdEnum

	// if enable SCRAM login verify
	EnableScram bool
}

// SetPriority sets the priority of the task.
func (f *BehaviorOptions) SetPriority(p int) *BehaviorOptions {
	f.Priority = &p
	return f
}

// SetParallelism sets the parallelism of the task.
func (f *BehaviorOptions) SetParallelism(p int) *BehaviorOptions {
	f.Parallelism = &p
	return f
}

// SetFetchSize sets the fetchSize of the task.
func (f *BehaviorOptions) SetFetchSize(fs int) *BehaviorOptions {
	f.FetchSize = &fs
	return f
}

func (f *BehaviorOptions) SetTryReconnectNums(n int) *BehaviorOptions {
	f.TryReconnectNums = &n
	return f
}

// SetSqlStd sets the SQL standard of the session.
func (f *BehaviorOptions) SetSqlStd(sqlStd SqlStdEnum) *BehaviorOptions {
	f.SqlStd = sqlStd
	return f
}

// GetPriority gets the priority of the task.
func (f *BehaviorOptions) GetPriority() int {
	if f.Priority == nil {
		return 4
	}
	return *f.Priority
}

// GetParallelism gets the parallelism of the task.
func (f *BehaviorOptions) GetParallelism() int {
	if f.Parallelism == nil {
		return 64
	}
	return *f.Parallelism
}

// GetFetchSize gets the fetchSize of the task.
func (f *BehaviorOptions) GetFetchSize() int {
	if f.FetchSize == nil {
		return 0
	}
	return *f.FetchSize
}

func (f *BehaviorOptions) GetTryReconnectNums() int {
	if f.TryReconnectNums == nil {
		return 0
	}
	if *f.TryReconnectNums < 0 {
		return 0
	}
	return *f.TryReconnectNums
}
