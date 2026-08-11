package dialer

// ExecutionTrace describes what happened while executing a request.
type ExecutionTrace struct {
	Failovers []FailoverTrace
}

// FailoverReason describes why the client moved from one node to another.
type FailoverReason string

const (
	FailoverReasonUnknown                FailoverReason = ""
	FailoverReasonNotLeader              FailoverReason = "NotLeader"
	FailoverReasonHighAvailability       FailoverReason = "HighAvailability"
	FailoverReasonServerDirectedFallback FailoverReason = "ServerDirectedFallback"
)

// FailoverTrace records one request-level failover observed during execution.
type FailoverTrace struct {
	Reason FailoverReason
	From   string
	To     string
	Err    string
}

func newExecutionTrace() *ExecutionTrace {
	return &ExecutionTrace{}
}
