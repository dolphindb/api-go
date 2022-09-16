package dialer

import (
	"bytes"
	"strconv"

	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/dolphindb/api-go/model"
)

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

func generatorRequestFlag(clear bool) int {
	flag := 0
	if clear {
		flag += 16
	}

	return flag
}
