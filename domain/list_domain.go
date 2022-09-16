package domain

import (
	"errors"
	"fmt"

	"github.com/dolphindb/api-go/model"
)

// ListDomain implements the Domain interface.
// You can use it to calculate partition keys with LIST partitionType.
type ListDomain struct {
	dict map[string]int

	dt  model.DataTypeByte
	cat model.CategoryString
}

// NewListDomain inits a ListDomain object.
func NewListDomain(vct *model.Vector, d model.DataTypeByte, cat model.CategoryString) (*ListDomain, error) {
	ld := &ListDomain{
		dt:   d,
		cat:  cat,
		dict: make(map[string]int),
	}

	if vct.GetDataType() != model.DtAny {
		return nil, errors.New("the input list must be a tuple")
	}

	row := vct.Rows()
	for i := 0; i < row; i++ {
		cur := vct.Data.ElementValue(i).(model.DataForm)
		if cur.GetDataForm() == model.DfScalar {
			s := cur.(*model.Scalar)
			key := s.DataType.String()
			ld.dict[key] = i
		} else {
			vec := cur.(*model.Vector)
			r := vec.Rows()
			for j := 0; j < r; j++ {
				key := vec.Data.ElementString(j)
				ld.dict[key] = i
			}
		}
	}

	return ld, nil
}

// GetPartitionKeys returns partition keys for partitioned table append.
func (l *ListDomain) GetPartitionKeys(partitionCol *model.Vector) ([]int, error) {
	pdt := getVectorRealDataType(partitionCol)
	if l.cat != model.GetCategory(pdt) {
		return nil, errors.New("data category incompatible")
	}

	if l.cat == model.TEMPORAL && l.dt != pdt {
		df, err := model.CastDateTime(partitionCol, l.dt)
		if err != nil {
			return nil, fmt.Errorf("can't convert type from %s to %s",
				model.GetDataTypeString(pdt), model.GetDataTypeString(l.dt))
		}

		partitionCol = df.(*model.Vector)
	}

	row := partitionCol.Rows()
	res := make([]int, row)
	for i := 0; i < row; i++ {
		key := partitionCol.Data.ElementString(i)
		ind, ok := l.dict[key]
		if !ok {
			res[i] = -1
		} else {
			res[i] = ind
		}
	}

	return res, nil
}
