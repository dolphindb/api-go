package streaming

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"time"

	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/dolphindb/api-go/v3/dialer/protocol"
	"github.com/dolphindb/api-go/v3/model"
)

type messageParser struct {
	ctx context.Context
	net.Conn
	*subscriber

	topic            string
	topicNameToIndex map[string]map[string]int
	firstColType     model.DataTypeByte
	isReversed       bool
}

func closeUnboundedChan(q *UnboundedChan) {
	if q == nil {
		return
	}
	q.Close()
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
	err := m.parse()

	// TODO concern more than one topic
	if IsClosed(m.topic) {
		raw, ok := queueMap.Load(m.topic)
		if ok && raw != nil {
			// HACK close queue at message parser & reconnect place, if not close here, then close at reconnect place
			queueMap.Delete(m.topic)
			haTopicToTrueTopic.Delete(m.topic)
			trueTopicToRequests.Delete(m.topic)
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
		streamingLogErrorf("failed to read msgID from conn: %v", err)
		return 0, err
	}

	msgID := bo.Uint64(bytes[8:])
	bytes, err = r.ReadBytes(protocol.StringSep)
	if err != nil {
		streamingLogErrorf("failed to read topic from conn: %v", err)
		return 0, err
	}

	m.topic = string(bytes)

	return msgID, nil
}

func (m *messageParser) parse() error {
	var r protocol.Reader
	if m.isReversed {
		r = m.Conn.(dialer.Conn).GetReader()
	} else {
		r = protocol.NewReader(m.Conn)
	}
	m.Conn.SetDeadline(time.Time{})
	for !m.IsClosed() {
		b, err := r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return err
			}

			streamingLogErrorf("failed to read ByteOrder byte from conn: %v", err)
			return err
		}

		bo := protocol.GetByteOrder('1')
		if b != 1 {
			bo = protocol.BigEndian
		}

		msgID, err := m.parseHeader(r, bo)
		if err != nil {
			streamingLogErrorf("failed to parse header: %v", err)
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
		streamingLogErrorf("failed to parse DataForm: %v", err)
		return err
	}

	switch {
	case df.GetDataForm() == model.DfTable && df.Rows() == 0:
		m.parseTable(df.(*model.Table))
	case df.GetDataForm() == model.DfVector:
		m.parseVector(msgID, df.(*model.Vector))
	default:
		streamingLogErrorf("invalid format in the message body; vector or table is expected")
	}

	return nil
}

func (m *messageParser) parseTable(tb *model.Table) {
	for _, v := range strings.Split(m.topic, ",") {
		// TODO bad design
		setReconnectItem(v, 0)
	}

	if tb == nil || tb.Columns() == 0 {
		streamingLogWarnf("ignore empty schema table for topic %q", m.topic)
		return
	}

	nameToIndex := make(map[string]int)
	count := 0
	for _, v := range tb.ColNames {
		nameToIndex[strings.ToLower(v)] = count
		count++
	}
	m.firstColType = tb.GetColumnByIndex(0).GetDataType()
	m.topicNameToIndex[m.topic] = nameToIndex
}

func (m *messageParser) isTupleMsg(firstElement model.DataForm) bool {
	if (firstElement.GetDataForm() == model.DfScalar) || (firstElement.GetDataType() == m.firstColType-64) {
		return true
	}
	return false
}

func (m *messageParser) parseVector(msgID uint64, vct *model.Vector) {
	colSize := vct.Rows()
	if vct == nil || colSize == 0 {
		streamingLogWarnf("ignore empty vector payload for topic %q", m.topic)
		return
	}

	first, ok := vct.Data.ElementValue(0).(model.DataForm)
	if !ok || first == nil {
		streamingLogWarnf("ignore invalid vector payload for topic %q", m.topic)
		return
	}

	rowSize := first.Rows()
	// form := vct.Data.ElementValue(0).(model.DataForm).GetDataForm()
	if m.isTupleMsg(first) {
		dispatch(m.generateMessage(int64(msgID), vct))
	} else {
		m.parseVectorWithMultiRows(rowSize, colSize, msgID, vct)
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
