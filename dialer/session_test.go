package dialer

import (
	"bufio"
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dolphindb/api-go/v3/dialer/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunInternalBindsCurrentSessionIDAtSendTime(t *testing.T) {
	rawConn, err := NewConn(context.TODO(), "127.0.0.1:8848", nil)
	require.NoError(t, err)
	c := rawConn.(*conn)

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	c.Conn = clientConn
	c.reader = protocol.NewReader(clientConn)
	c.isConnected = true
	c.sessionID = []byte("current-session")
	c.timeout = time.Second

	type capturedRequest struct {
		header string
		err    error
	}
	captured := make(chan capturedRequest, 1)

	go func() {
		reader := bufio.NewReader(serverConn)
		header, err := reader.ReadString('\n')
		if err != nil {
			captured <- capturedRequest{err: err}
			return
		}
		header = strings.TrimSuffix(header, "\n")

		fields := strings.Fields(header)
		if len(fields) < 3 {
			captured <- capturedRequest{header: header, err: io.ErrUnexpectedEOF}
			return
		}
		commandLength, err := strconv.Atoi(fields[2])
		if err != nil {
			captured <- capturedRequest{header: header, err: err}
			return
		}
		if _, err = io.CopyN(io.Discard, reader, int64(commandLength)); err != nil {
			captured <- capturedRequest{header: header, err: err}
			return
		}
		if _, err = serverConn.Write([]byte("response-session 0 1\nOK\n")); err != nil {
			captured <- capturedRequest{header: header, err: err}
			return
		}

		captured <- capturedRequest{header: header}
	}()

	_, _, err = c.runInternal(&requestParams{
		commandType: functionCmd,
		Command:     generateFunctionCommand("tableInsert{t}", defaultByteOrder, nil),
		SessionID:   []byte("stale-session"),
		ByteOrder:   defaultByteOrder,
	})
	require.NoError(t, err)

	request := <-captured
	require.NoError(t, request.err)
	assert.Contains(t, request.header, "API2 current-session ")
	assert.NotContains(t, request.header, "stale-session")
	assert.Equal(t, "response-session", c.GetSession())
}
