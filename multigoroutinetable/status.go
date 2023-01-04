package multigoroutinetable

import (
	"fmt"
	"strings"
)

// Status is used to store the status of MultiGoroutineTable.
type Status struct {
	// errMsg of MultiGoroutineTable
	ErrMsg string
	// the number of records that failed to be sent
	FailedRows int
	// the number of records that have been sent
	SentRows int
	// Deprecated.
	UnSentRows int
	// the number of unsent records
	UnsentRows int
	// check whether the MultiGoroutineTable finished
	IsExit bool
	// Deprecated.
	GoroutineStatusList []*GoroutineStatus
	// list the status of goroutines
	GoroutineStatus []*GoroutineStatus
}

// String returns the status of goroutines in string format.
func (s *Status) String() string {
	by := strings.Builder{}
	by.WriteString(fmt.Sprintf("errMsg         :  %s\n", s.ErrMsg))
	by.WriteString(fmt.Sprintf("isExit         :  %v\n", s.IsExit))
	by.WriteString(fmt.Sprintf("sentRows       :  %d\n", s.SentRows))
	by.WriteString(fmt.Sprintf("unsentRows     :  %d\n", s.UnsentRows))
	by.WriteString(fmt.Sprintf("sendFailedRows :  %d\n", s.FailedRows))
	by.WriteString("goroutineStatus   :\n")
	for _, v := range s.GoroutineStatus {
		by.WriteString(fmt.Sprintf("    %s\n", v.String()))
	}

	return by.String()
}

// GoroutineStatus records the status of goroutine.
type GoroutineStatus struct {
	// goroutine index
	GoroutineIndex int
	// the number of records that failed to be sent
	FailedRows int
	// the number of records that have been sent
	SentRows int
	// Deprecated.
	UnSentRows int
	// the number of unsent records
	UnsentRows int
}

// String returns the status of goroutines in string format.
func (ts *GoroutineStatus) String() string {
	return fmt.Sprintf("goroutineIndex: %d, sentRows: %d, unsentRows: %d, sendFailedRows: %d",
		ts.GoroutineIndex, ts.SentRows, ts.UnsentRows, ts.FailedRows)
}
