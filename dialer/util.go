package dialer

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/dolphindb/api-go/model"
)

const (
	IGNORE ErrorType = iota
	UNKNOW
	NEWLEADER
	NODENOTAVAIL
	NOINITIALIZED
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

	if opt.IsReverseStreaming {
		flag += 131072
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

	fl, err := os.Open(path)
	if err != nil {
		return "", err
	}

	defer fl.Close()

	byt, err := ioutil.ReadAll(fl)
	if err != nil {
		return "", err
	}

	return string(byt), err
}

func parseAddr(raw string) string {
	strs := strings.Split(raw, ":")
	if len(strs) < 2 {
		return ""
	}

	return strings.Join(strs[:2], ":")
}
