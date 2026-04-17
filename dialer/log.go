package dialer

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

var (
	dialerLogMu     sync.Mutex
	dialerLogWriter io.Writer = os.Stdout
)

func dialerLogf(format string, args ...interface{}) {
	dialerLogMu.Lock()
	defer dialerLogMu.Unlock()

	prefixArgs := make([]interface{}, 0, len(args)+1)
	prefixArgs = append(prefixArgs, time.Now().Format("2006-01-02 15:04:05.000"))
	prefixArgs = append(prefixArgs, args...)
	fmt.Fprintf(dialerLogWriter, "%s [dialer] "+format+"\n", prefixArgs...)
}
