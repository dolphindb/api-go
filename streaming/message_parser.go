package streaming

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/dolphindb/api-go/dialer"
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
func closeUnboundedChan(q *UnboundedChan) {
	close(q.In)
	// y := 0
	// for {
	// 	select {
	// 	case <-q.Out:
	// 		y++
	// 		// fmt.Println("drain", y)
	// 	default:
	// 		return
	// 	}
	// }
}

func (m *messageParser) run() {
	err := m.parse();

	// TODO concern more than one topic
	if IsClosed(m.topic) {
		raw, ok := queueMap.Load(m.topic)
		if ok && raw != nil {
			// HACK close queue at message parser & reconnect place, if not close here, then close at reconnect place
			queueMap.Delete(m.topic)
			haTopicToTrueTopic.Delete(m.topic)
			trueTopicToSites.Delete(m.topic)
			q := raw.(*UnboundedChan)
			closeUnboundedChan(q)
		}
	}

	// TODO if m.topic is not ready, but connection is over, how to make sure it know if it should reconnect or not
	if err != nil && !IsClosed(m.topic) {
		setReconnectItem(m.topic, 1)
	}
}

func (m *messageParser) parseHeader(r protocol.Reader, bo protocol.ByteOrder) (uint64, error) {
	bytes, err := r.ReadCertainBytes(16)
	if err != nil {
		fmt.Printf("Failed to read msgID from conn: %s\n", err.Error())
		return 0, err
	}

	msgID := bo.Uint64(bytes[8:])
	bytes, err = r.ReadBytes(protocol.StringSep)
	if err != nil {
		fmt.Printf("Failed to read topic from conn: %s\n", err.Error())
		return 0, err
	}

	m.topic = string(bytes)

	return msgID, nil
}

func (m *messageParser) parse() error {
	r := m.Conn.(dialer.Conn).GetReader()
	// r := protocol.NewReader(m.Conn)
	m.Conn.SetDeadline(time.Time{})
	for !m.IsClosed() {
		b, err := r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return err
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

		err = m.parseData(msgID, r, bo)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *messageParser) parseData(msgID uint64, r protocol.Reader, bo protocol.ByteOrder) error {
	df, err := model.ParseDataForm(r, bo)
	if err != nil {
		fmt.Printf("Failed to parse DataForm: %s\n", err.Error())
		return err
	}

	switch {
	case df.GetDataForm() == model.DfTable && df.Rows() == 0:
		m.parseTable(df.(*model.Table))
	case df.GetDataForm() == model.DfVector:
		m.parseVector(msgID, df.(*model.Vector))
	default:
		fmt.Println("Invalid format in the message body. Vector or table is expected")
	}

	return nil
}

func (m *messageParser) parseTable(tb *model.Table) {
	for _, v := range strings.Split(m.topic, ",") {
		setReconnectItem(v, 0)
	}

	nameToIndex := make(map[string]int)
	count := 0
	for _, v := range tb.ColNames {
		nameToIndex[strings.ToLower(v)] = count
		count++
	}

	m.topicNameToIndex[m.topic] = nameToIndex
}

func (m *messageParser) parseVector(msgID uint64, vct *model.Vector) {
	colSize := vct.Rows()
	rowSize := vct.Data.ElementValue(0).(model.DataForm).Rows()
	if rowSize > 1 {
		m.parseVectorWithMultiRows(rowSize, colSize, msgID, vct)
	} else if rowSize == 1 {
		dispatch(m.generateMessage(int64(msgID), vct))
	}
}

func (m *messageParser) parseVectorWithMultiRows(rowSize, colSize int, msgID uint64, vct *model.Vector) {
	msgs := make([]IMessage, rowSize)
	st := msgID - uint64(rowSize) + 1
	for i := 0; i < rowSize; i++ {
		msgs[i] = m.generateMessage(int64(st), repackVector(i, colSize, vct))
	}

	batchDispatch(msgs)
}

func repackVector(ind, colSize int, vct *model.Vector) *model.Vector {
	dts := make([]model.DataForm, colSize)
	for j := 0; j < colSize; j++ {
		df := vct.Data.ElementValue(j).(*model.Vector)
		if df.GetDataType() > 64 && df.GetDataType() < 128 {
			d, _ := model.NewDataType(model.DtAny, df.GetVectorValue(ind))
			dts[j] = model.NewScalar(d)
		} else {
			dts[j] = model.NewScalar(df.Get(ind))
		}
	}

	dtl, _ := model.NewDataTypeListFromRawData(model.DtAny, dts)
	return model.NewVector(dtl)
}

func (m *messageParser) generateMessage(offset int64, vct *model.Vector) *Message {
	topics := strings.Split(m.topic, ",")
	return &Message{
		offset:      offset,
		topic:       m.topic,
		msg:         vct,
		nameToIndex: m.topicNameToIndex[topics[0]], // TODO why use first one ?
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
