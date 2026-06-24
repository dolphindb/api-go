package multigoroutinetable

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/dolphindb/api-go/v3/model"
)

type preparedBatchColumn struct {
	vector *model.Vector
	count  int
	first  interface{}
	empty  interface{}
}

func prepareBatchColumn(dt model.DataTypeByte, raw interface{}) (*preparedBatchColumn, error) {
	switch {
	case dt >= 128:
		return prepareExtendedBatchColumn(dt, raw)
	case dt >= 64:
		return prepareArrayBatchColumn(dt, raw)
	case dt == model.DtDecimal32:
		return prepareDecimal32BatchColumn(raw)
	case dt == model.DtDecimal64:
		return prepareDecimal64BatchColumn(raw)
	case dt == model.DtDecimal128:
		return prepareDecimal128BatchColumn(raw)
	default:
		return prepareStandardBatchColumn(dt, raw)
	}
}

func prepareStandardBatchColumn(dt model.DataTypeByte, raw interface{}) (*preparedBatchColumn, error) {
	count, first, empty, err := batchSliceInfo(raw)
	if err != nil {
		return nil, err
	}

	dtl, err := model.NewDataTypeListFromRawData(dt, raw)
	if err != nil {
		return nil, err
	}

	return &preparedBatchColumn{
		vector: model.NewVector(dtl),
		count:  count,
		first:  first,
		empty:  empty,
	}, nil
}

func prepareDecimal32BatchColumn(raw interface{}) (*preparedBatchColumn, error) {
	vec, ok := raw.([]*model.Decimal32)
	if !ok {
		return nil, errors.New("the type of input must be []*model.Decimal32 when datatype is DtDecimal32")
	}

	dtl := model.NewEmptyDataTypeList(model.DtDecimal32, len(vec))
	for ind, value := range vec {
		if err := dtl.SetWithRawData(ind, value); err != nil {
			return nil, err
		}
	}

	var first interface{}
	if len(vec) > 0 {
		first = vec[0]
	}

	return &preparedBatchColumn{
		vector: model.NewVector(dtl),
		count:  len(vec),
		first:  first,
		empty:  vec[:0],
	}, nil
}

func prepareDecimal64BatchColumn(raw interface{}) (*preparedBatchColumn, error) {
	vec, ok := raw.([]*model.Decimal64)
	if !ok {
		return nil, errors.New("the type of input must be []*model.Decimal64 when datatype is DtDecimal64")
	}

	dtl := model.NewEmptyDataTypeList(model.DtDecimal64, len(vec))
	for ind, value := range vec {
		if err := dtl.SetWithRawData(ind, value); err != nil {
			return nil, err
		}
	}

	var first interface{}
	if len(vec) > 0 {
		first = vec[0]
	}

	return &preparedBatchColumn{
		vector: model.NewVector(dtl),
		count:  len(vec),
		first:  first,
		empty:  vec[:0],
	}, nil
}

func prepareDecimal128BatchColumn(raw interface{}) (*preparedBatchColumn, error) {
	vec, ok := raw.([]*model.Decimal128)
	if !ok {
		return nil, errors.New("the type of input must be []*model.Decimal128 when datatype is DtDecimal128")
	}

	dtl := model.NewEmptyDataTypeList(model.DtDecimal128, len(vec))
	for ind, value := range vec {
		if err := dtl.SetWithRawData(ind, value); err != nil {
			return nil, err
		}
	}

	var first interface{}
	if len(vec) > 0 {
		first = vec[0]
	}

	return &preparedBatchColumn{
		vector: model.NewVector(dtl),
		count:  len(vec),
		first:  first,
		empty:  vec[:0],
	}, nil
}

func prepareArrayBatchColumn(dt model.DataTypeByte, raw interface{}) (*preparedBatchColumn, error) {
	vec, ok := raw.([]model.DataType)
	if !ok {
		return nil, errors.New("the type of input must be []model.DataType when datatype is array vector")
	}

	vl := make([]*model.Vector, len(vec))
	for ind, value := range vec {
		if value == nil {
			return nil, fmt.Errorf("array vector item %d must not be nil", ind)
		}

		df, ok := value.Value().(model.DataForm)
		if !ok {
			return nil, fmt.Errorf("array vector item %d must wrap a vector, got %T", ind, value.Value())
		}

		vct, ok := df.(*model.Vector)
		if !ok {
			return nil, fmt.Errorf("array vector item %d must wrap a vector, got %T", ind, df)
		}

		vl[ind] = vct
	}

	var (
		vct *model.Vector
		err error
	)
	if len(vl) > 0 {
		av := model.NewArrayVector(vl)
		vct, err = model.NewVectorWithArrayVector(av)
		if err != nil {
			return nil, err
		}
	}

	var first interface{}
	if len(vec) > 0 {
		first = vec[0]
	}

	return &preparedBatchColumn{
		vector: vct,
		count:  len(vec),
		first:  first,
		empty:  vec[:0],
	}, nil
}

func prepareExtendedBatchColumn(dt model.DataTypeByte, raw interface{}) (*preparedBatchColumn, error) {
	count, first, empty, err := batchSliceInfo(raw)
	if err != nil {
		return nil, err
	}

	dtl := model.NewEmptyDataTypeList(dt-128, count)
	return &preparedBatchColumn{
		vector: model.NewVector(dtl),
		count:  count,
		first:  first,
		empty:  empty,
	}, nil
}

func batchSliceInfo(raw interface{}) (int, interface{}, interface{}, error) {
	if raw == nil {
		return 0, nil, nil, errors.New("batch column must be a slice, got <nil>")
	}

	rv := reflect.ValueOf(raw)
	if rv.Kind() != reflect.Slice {
		return 0, nil, nil, fmt.Errorf("batch column must be a slice, got %T", raw)
	}

	count := rv.Len()
	empty := reflect.MakeSlice(rv.Type(), 0, rv.Cap()).Interface()

	if count == 0 {
		return 0, nil, empty, nil
	}

	return count, rv.Index(0).Interface(), empty, nil
}
