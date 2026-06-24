package dialer

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/dolphindb/api-go/v3/dialer/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseResponseHeaderPrintsServerMessagesToStdout(t *testing.T) {
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	defer func() {
		os.Stdout = oldStdout
	}()

	reader := protocol.NewReader(strings.NewReader("MSG\nline 1\nline 2\n\x00session 0 0\n"))
	header, err := (&conn{}).parseResponseHeader(reader)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	output, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())

	assert.Equal(t, "line 1\nline 2\n", string(output))
	assert.Equal(t, []byte("session"), header.sessionID)
	assert.Equal(t, 0, header.objectCount)
}
