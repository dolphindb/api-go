package domain

import (
	"errors"
	"fmt"

	"github.com/dolphindb/api-go/model"
)

// RangeDomain implements the Domain interface.
// You can use it to calculate partition keys with RANGE partitionType.
type RangeDomain struct {
	rangeVector *model.Vector
	dt          model.DataTypeByte
	cat         model.CategoryString
}

// GetPartitionKeys returns partition keys for partitioned table append.
func (r *RangeDomain) GetPartitionKeys(partitionCol *model.Vector) ([]int, error) {
	pdt := getVectorRealDataType(partitionCol)
	if r.cat != model.GetCategory(pdt) {
		return nil, errors.New("data category incompatible")
	}

	cg := model.GetCategory(r.dt)
	if cg == model.TEMPORAL && r.dt != partitionCol.GetDataType() {
		df, err := model.CastDateTime(partitionCol, r.dt)
		if err != nil {
			return nil, fmt.Errorf("can't convert type from %s to %s",
				model.GetDataTypeString(pdt), model.GetDataTypeString(r.dt))
		}

		partitionCol = df.(*model.Vector)
	}

	partitions := r.rangeVector.Rows() - 1
	row := partitionCol.Rows()
	res := make([]int, row)
	for i := 0; i < row; i++ {
		ind := r.rangeVector.AsOf(partitionCol.Data.Get(i))
		if ind >= partitions {
			res[i] = -1
		} else {
			res[i] = ind
		}
	}

	return res, nil
}
