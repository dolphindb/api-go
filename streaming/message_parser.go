package streaming

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/dolphindb/api-go/model"
)

type messageParser struct {
	ctx context.Context
	net.Conn
	*subscriber

	topic            string
	topicNameToIndex map[string]map[string]int
}

func (m *messageParser) run() {
	if err := m.parse(); err != nil {
		if IsClosed(m.topic) {
			return
		}

		setNeedReconnect(m.topic, 1)
	}
}

func (m *messageParser) parseHeader(r protocol.Reader, bo protocol.ByteOrder) (uint64, error) {
	byts, err := r.ReadCertainBytes(16)
	if err != nil {
		fmt.Printf("Failed to read msgID from conn: %s\n", err.Error())
		return 0, err
	}

	msgID := bo.Uint64(byts[8:])
	byts, err = r.ReadBytes(protocol.StringSep)
	if err != nil {
		fmt.Printf("Failed to read topic from conn: %s\n", err.Error())
		return 0, err
	}

	m.topic = string(byts)

	return msgID, nil
}

func (m *messageParser) parse() error {
	r := protocol.NewReader(m.Conn)
	for !m.IsClosed() {
		b, err := r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			fmt.Printf("Failed to read ByteOrder byte from conn: %s\n", err.Error())
			return err
		}

		bo := protocol.GetByteOrder('1')
		if b != 1 {
			bo = protocol.BigEndian
		}

		msgID, err := m.parseHeader(r, bo)
		if err != nil {
			fmt.Printf("Failed to parse header: %s\n", err.Error())
			return err
		}

		df, err := model.ParseDataForm(r, bo)
		if err != nil {
			fmt.Printf("Failed to parse DataForm: %s\n", err.Error())
			return err
		}

		switch {
		case df.GetDataForm() == model.DfTable:
			m.handleTable(df.(*model.Table))
		case df.GetDataForm() == model.DfVector:
			m.handleVector(msgID, df.(*model.Vector))
		default:
			fmt.Println("Invalid format in the message body. Vector or table is expected")
		}
	}

	return nil
}

func (m *messageParser) handleTable(tb *model.Table) {
	if tb.Rows() != 0 {
		fmt.Println("Invalid format in the message body. Vector or table is expected")
		return
	}

	for _, v := range strings.Split(m.topic, ",") {
		setNeedReconnect(v, 0)
	}

	nameToIndex := make(map[string]int)
	count := 0
	for _, v := range tb.ColNames {
		nameToIndex[strings.ToLower(v)] = count
		count++
	}

	m.topicNameToIndex[m.topic] = nameToIndex
}

func (m *messageParser) handleVector(msgID uint64, vct *model.Vector) {
	colSize := vct.Rows()
	rowSize := vct.Data.ElementValue(0).(model.DataForm).Rows()
	if rowSize > 1 {
		msgs := make([]IMessage, rowSize)
		st := msgID - uint64(rowSize) + 1
		for i := 0; i < rowSize; i++ {
			dts := make([]model.DataForm, colSize)
			for j := 0; j < colSize; j++ {
				df := vct.Data.ElementValue(j).(*model.Vector)
				if df.GetDataType() > 64 && df.GetDataType() < 128 {
					d, _ := model.NewDataType(model.DtAny, df.GetVectorValue(i))
					dts[j] = model.NewScalar(d)
				} else {
					dts[j] = model.NewScalar(df.Get(i))
				}
			}

			dtl, _ := model.NewDataTypeListWithRaw(model.DtAny, dts)
			topics := strings.Split(m.topic, ",")
			msg := &Message{
				offset:      int64(st),
				topic:       m.topic,
				msg:         model.NewVector(dtl),
				nameToIndex: m.topicNameToIndex[topics[0]],
			}

			msgs[i] = msg
		}

		batchDispatch(msgs)
	} else if rowSize == 1 {
		topics := strings.Split(m.topic, ",")
		msg := &Message{
			offset:      int64(msgID),
			topic:       m.topic,
			msg:         vct,
			nameToIndex: m.topicNameToIndex[topics[0]],
		}

		dispatch(msg)
	}
}

func (m *messageParser) IsClosed() bool {
	select {
	case <-m.ctx.Done():
		return true
	default:
		return false
	}
}
