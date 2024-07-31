package dialer

// BehaviorOptions helps you configure behavior identity.
// Refer to https://github.com/dolphindb/Tutorials_CN/blob/master/api_protocol.md#254-%E8%A1%8C%E4%B8%BA%E6%A0%87%E8%AF%86 for more details.
type BehaviorOptions struct {
	// Priority specifies the priority of the task
	Priority *int
	// Parallelism specifies the parallelism of the task
	Parallelism *int
	// FetchSize specifies the fetchSize of the task
	FetchSize *int

	// Whether to enable load balancing.
	// If true, connect to the address with the fewest connections.
	LoadBalance bool

	// Whether to enable high availability.
	// If true, when the address is unrearched, another address in HighAvailabilitySites will be connected.
	EnableHighAvailability bool

	// Available only when EnableHighAvailability is true.
	HighAvailabilitySites []string

	// If true, the address will be reconncted util the server is ready.
	Reconnect bool

	// IsReverseStreaming specifies whether the job is a reverse stream subscription
	IsReverseStreaming bool
	// IsClearSessionMemory specifies whether to clear session memory after the job
	IsClearSessionMemory bool
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
