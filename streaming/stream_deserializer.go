package streaming

import (
	"errors"
	"fmt"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
)

type StreamDeserializer struct {
	msgDeserializerMap map[string]*msgDeserializer
}

type StreamDeserializerOption struct {
	// Filters    interface{}
	TableNames map[string][2]string
	Conn       api.DolphinDB
}

func NewStreamDeserializer(opt *StreamDeserializerOption) (*StreamDeserializer, error) {
	var err error
	sd := &StreamDeserializer{}
	// if opt.Filters != nil {
	// 	sd.msgDeserializerMap, err = initWithFilters(opt.Filters)
	// 	return sd, err
	// }

	if opt.Conn != nil {
		sd.msgDeserializerMap, err = initWithConn(opt)
	}

	return sd, err
}

func initWithConn(opt *StreamDeserializerOption) (map[string]*msgDeserializer, error) {
	if opt.TableNames == nil {
		return nil, errors.New("the TableNames is null")
	}

	var schema *model.Dictionary
	filters := make(map[string]*model.Dictionary)
	for k, v := range opt.TableNames {
		dbName := v[0]
		tbName := v[1]
		if dbName == "" {
			raw, err := opt.Conn.RunScript(fmt.Sprintf("schema(%s)", tbName))
			if err != nil {
				return nil, err
			}

			schema = raw.(*model.Dictionary)
		} else {
			raw, err := opt.Conn.RunScript(fmt.Sprintf("schema(loadTable(\"%s\",\"%s\"))", dbName, tbName))
			if err != nil {
				return nil, err
			}

			schema = raw.(*model.Dictionary)
		}

		filters[k] = schema
	}

	return initWithSchema(filters)
}

// func initWithFilters(filters interface{}) (map[string]*msgDeserializer, error) {
// 	var err error
// 	res := make(map[string]*msgDeserializer)
// 	switch r := filters.(type) {
// 	case map[string][]model.DataTypeByte:
// 		for k, v := range r {
// 			if v == nil {
// 				return res, errors.New("The colTypes value can not be null")
// 			}

// 			res[k] = newMsgDeserializer(v)
// 		}
// 	case map[string]*model.Dictionary:
// 		res, err = initWithSchema(r)
// 	default:
// 		return nil, errors.New("Invalid filter type.")
// 	}

// 	return res, err
// }

func initWithSchema(filters map[string]*model.Dictionary) (map[string]*msgDeserializer, error) {
	res := make(map[string]*msgDeserializer)
	for k, v := range filters {
		if v == nil {
			return nil, errors.New("the schema value can not be null")
		}

		val, err := v.Get("colDefs")
		if err != nil {
			return nil, err
		}
		t := val.Value().(*model.Table)

		nameVector := t.GetColumnByName("name")
		nameRaw := nameVector.Data.Value()
		colNames := make([]string, len(nameRaw))
		for k, v := range nameRaw {
			colNames[k] = v.(string)
		}

		dtiVct := t.GetColumnByName("typeInt")
		raw := dtiVct.Data.Value()
		colTypes := make([]model.DataTypeByte, len(raw))

		for k, v := range raw {
			colTypes[k] = model.DataTypeByte(v.(int32))
		}

		res[k] = newMsgDeserializer(colNames, colTypes)
	}

	return res, nil
}

func (sd *StreamDeserializer) Parse(msg IMessage) (*Message, error) {
	if msg.Size() < 3 {
		return nil, errors.New("the data must contain 3 columns")
	}

	secondDt := msg.GetValue(1).GetDataType()
	if secondDt != model.DtSymbol && secondDt != model.DtString {
		return nil, errors.New("the 2rd column must be a vector type with symbol or string")
	}

	thirdDt := msg.GetValue(2).GetDataType()
	if thirdDt != model.DtBlob {
		return nil, errors.New("the 3rd column must be a vector type with blob")
	}

	sym := msg.GetValue(1).(*model.Scalar).DataType.String()
	blob := msg.GetValue(2).(*model.Scalar).DataType.Value().([]byte)
	if sd.msgDeserializerMap == nil {
		return nil, errors.New("the StreamDeserialize has not init yet")
	}
	if sd.msgDeserializerMap[sym] == nil {
		return nil, fmt.Errorf("the filter %s does not exist", sym)
	}

	md:= sd.msgDeserializerMap[sym]

	vct, err := md.parse(blob)
	if err != nil {
		return nil, err
	}

	return &Message{
		offset: msg.GetOffset(),
		topic:  msg.GetTopic(),
		msg:    vct,
		sym:    sym,
		nameToIndex: md.nameToIndex,
	}, nil
}
