package streaming

import (
	"bytes"

	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/dolphindb/api-go/model"
)

type msgDeserializer struct {
	colTypes []model.DataTypeByte
	nameToIndex map[string]int
}

func newMsgDeserializer(colNames []string, colTypes []model.DataTypeByte) *msgDeserializer {
	// init nameToIndex
	nameToIndex := make(map[string]int, len(colNames))
	for k,v := range colNames {
		nameToIndex[v] = k
	}
	md := &msgDeserializer{
		nameToIndex: nameToIndex,
		colTypes: make([]model.DataTypeByte, len(colTypes)),
	}

	copy(md.colTypes, colTypes)
	return md
}

func (md *msgDeserializer) parse(data []byte) (*model.Vector, error) {
	buf := bytes.NewBuffer(data)
	rd := protocol.NewReader(buf)

	scalarList := make([]model.DataForm, len(md.colTypes))
	for k, v := range md.colTypes {
		dt, err := model.ParseDataType(rd, v, protocol.LittleEndian)
		if err != nil {
			return nil, err
		}

		scalarList[k] = model.NewScalar(dt)
	}

	dtl, err := model.NewDataTypeListFromRawData(model.DtAny, scalarList)
	if err != nil {
		return nil, err
	}

	return model.NewVector(dtl), nil
}
