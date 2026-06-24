package dialer

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dolphindb/api-go/v3/dialer/protocol"
	"github.com/dolphindb/api-go/v3/model"
)

const (
	IGNORE ErrorType = iota
	UNKNOWN
	NEW_LEADER
	UNKNOWN_LEADER
	NODE_NOT_AVAIL
	NO_INITIALIZED
	LOGIN_REQUIRED
	UNEXPECT
)

type ErrorType int

func generateScriptCommand(cmdStr string) []byte {
	bs := bytes.Buffer{}
	bs.WriteString(scriptCmd)
	bs.WriteByte(protocol.NewLine)
	bs.WriteString(cmdStr)
	return bs.Bytes()
}

func generateFunctionCommand(cmdStr string, bo byte, args []model.DataForm) []byte {
	bs := bytes.Buffer{}
	bs.WriteString(functionCmd)
	bs.WriteByte(protocol.NewLine)
	bs.WriteString(cmdStr)
	bs.WriteByte(protocol.NewLine)
	bs.WriteString(strconv.Itoa(len(args)))
	bs.WriteByte(protocol.NewLine)
	bs.WriteByte(bo)
	bs.WriteByte(protocol.NewLine)
	return bs.Bytes()
}

func generateConnectionCommand() []byte {
	bs := bytes.Buffer{}
	bs.WriteString(connectCmd)
	bs.WriteByte(protocol.NewLine)
	return bs.Bytes()
}

func generateVariableCommand(names string, bo byte, count int) []byte {
	bs := bytes.Buffer{}
	bs.WriteString(variableCmd)
	bs.WriteByte(protocol.NewLine)
	bs.WriteString(names)
	bs.WriteByte(protocol.NewLine)
	bs.WriteString(strconv.Itoa(count))
	bs.WriteByte(protocol.NewLine)
	bs.WriteByte(bo)
	return bs.Bytes()
}

func generatorRequestFlag(opt *BehaviorOptions) int {
	flag := 0
	if opt.IsClearSessionMemory {
		flag += 16
	}

	if opt.UsePython {
		flag += 2048
	}

	if opt.IsReverseStreaming {
		flag += 131072
	}

	if opt.SqlStd != SqlStdDolphinDB {
		flag += int(opt.SqlStd) << 19
	}
	return flag
}

func readFile(path string) (string, error) {
	var err error
	if !filepath.IsAbs(path) {
		path, err = filepath.Abs(path)
		if err != nil {
			return "", err
		}
	}

	byt, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	return string(byt), nil
}

func parseAddr(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	token := strings.Fields(raw)[0]
	if host, port, err := net.SplitHostPort(token); err == nil {
		return net.JoinHostPort(host, port)
	}

	lastColon := strings.LastIndex(token, ":")
	if lastColon < 0 {
		return ""
	}

	host, port, err := net.SplitHostPort(token[:lastColon])
	if err != nil {
		return ""
	}

	return net.JoinHostPort(host, port)
}

func extractTaggedAddr(msg, tag string) string {
	ind := strings.Index(msg, tag)
	if ind < 0 {
		return ""
	}

	return parseAddr(msg[ind+len(tag):])
}

var loginWithCredentials = Login

func Login(conn Conn, userID, password string) error {
	return conn.ConnLogin(userID, password)
}
