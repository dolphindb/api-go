package streaming

import (
	"net"
	"strconv"

	"github.com/dolphindb/api-go/v3/test/setup"
)

var (
	testStreamingAddress = setup.Address
	testStreamingHost    string
	testStreamingPort    int
)

func init() {
	host, port, err := net.SplitHostPort(testStreamingAddress)
	if err != nil {
		testStreamingHost = testStreamingAddress
		testStreamingPort = 8848
		return
	}

	testStreamingHost = host
	testStreamingPort, err = strconv.Atoi(port)
	if err != nil {
		testStreamingPort = 8848
	}
}
