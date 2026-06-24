package dialer

import (
	"errors"
	"slices"
	"time"
)

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
	// Priority specifies the priority of the task.
	// Nil uses the SDK default value.
	Priority *int
	// Parallelism specifies the parallelism of the task.
	// Nil uses the SDK default value.
	Parallelism *int
	// FetchSize specifies the fetchSize of the task.
	// Nil uses the SDK default value.
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

	// TryReconnectNums specifies the number of reconnection attempts.
	// Nil means reconnect indefinitely; any non-positive value is invalid.
	TryReconnectNums *int

	// UsePython specifies whether the session uses a Python parser
	UsePython bool

	// SqlStd specifies which SQL standard should be used for the session.
	SqlStd SqlStdEnum

	// EnableScram specifies whether SCRAM login is required.
	// When false, login still tries SCRAM first and falls back to password login
	// if the server or user does not support SCRAM.
	EnableScram bool
}

var errInvalidTryReconnectNums = errors.New("TryReconnectNums must be nil or greater than 0")

const (
	defaultPriority    = 4
	defaultParallelism = 64
	defaultFetchSize   = 0
)

// Validate checks whether the behavior options are internally consistent.
func (f *BehaviorOptions) Validate() error {
	if f == nil {
		return nil
	}

	return validateTryReconnectNums(f.TryReconnectNums)
}

func normalizeBehaviorOptions(opt *BehaviorOptions) (*BehaviorOptions, error) {
	if err := validateBehaviorOptions(opt); err != nil {
		return nil, err
	}

	if opt == nil {
		opt = &BehaviorOptions{}
	}

	normalized := *opt
	normalized.Priority = cloneIntPtrOrDefault(opt.Priority, defaultPriority)
	normalized.Parallelism = cloneIntPtrOrDefault(opt.Parallelism, defaultParallelism)
	normalized.FetchSize = cloneIntPtrOrDefault(opt.FetchSize, defaultFetchSize)
	normalized.TryReconnectNums = cloneIntPtr(opt.TryReconnectNums)
	if opt.HighAvailabilitySites != nil {
		normalized.HighAvailabilitySites = slices.Clone(opt.HighAvailabilitySites)
	}

	return &normalized, nil
}

func validateBehaviorOptions(opt *BehaviorOptions) error {
	if opt == nil {
		return nil
	}

	return validateTryReconnectNums(opt.TryReconnectNums)
}

func validateTryReconnectNums(n *int) error {
	if n != nil && *n <= 0 {
		return errInvalidTryReconnectNums
	}

	return nil
}

func cloneIntPtrOrDefault(value *int, fallback int) *int {
	if value == nil {
		value = &fallback
	}

	return cloneIntPtr(value)
}

func cloneIntPtr(value *int) *int {
	if value == nil {
		return nil
	}

	copied := *value
	return &copied
}
