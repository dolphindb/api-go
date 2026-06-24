package dialer

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/dolphindb/api-go/v3/dialer/protocol"
	"github.com/dolphindb/api-go/v3/model"
)

type requestParams struct {
	commandType string

	SessionID []byte
	ByteOrder byte
	Command   []byte
	Args      []model.DataForm
}

func writeRequest(wr *protocol.Writer, params *requestParams, opt *BehaviorOptions) error {
	writeHeader(wr, opt, params.SessionID, len(params.Command), params.commandType)
	err := writeCommand(wr, params.Command)
	if err != nil {
		return err
	}

	err = writeArgs(wr, params.ByteOrder, params.Args)
	if err != nil {
		return err
	}

	return wr.Flush()
}

func writeFlag(opt *BehaviorOptions) []byte {
	bs := bytes.Buffer{}

	bs.WriteString(fmt.Sprintf(" / %d_1_%d_%d", generatorRequestFlag(opt), *opt.Priority, *opt.Parallelism))
	if *opt.FetchSize > 0 {
		bs.WriteString(fmt.Sprintf("__%d", *opt.FetchSize))
	}

	return bs.Bytes()
}

func writeHeader(w *protocol.Writer, opt *BehaviorOptions, sessionID []byte, commandLength int, commandType string) {
	_ = w.Write(protocol.APIBytes)
	_ = w.WriteByte(protocol.EmptySpace)
	_ = w.Write(sessionID)
	_ = w.WriteByte(protocol.EmptySpace)
	_ = w.Write([]byte(strconv.Itoa(commandLength)))
	if commandType == scriptCmd || commandType == functionCmd || commandType == connectCmd {
		_ = w.Write(writeFlag(opt))
	}
	_ = w.WriteByte(protocol.NewLine)
}

func writeCommand(w *protocol.Writer, command []byte) error {
	return w.Write(command)
}

func writeArgs(w *protocol.Writer, bo byte, args []model.DataForm) error {
	b := protocol.GetByteOrder(bo)
	for _, arg := range args {
		if arg == nil {
			continue
		}

		err := arg.Render(w, b)
		if err != nil {
			return err
		}
	}

	return nil
}
