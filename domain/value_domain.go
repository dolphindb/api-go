package domain

import (
	"errors"
	"fmt"

	"github.com/dolphindb/api-go/model"
)

// ValueDomain implements the Domain interface.
// You can use it to calculate partition keys with VALUE partitionType.
type ValueDomain struct {
	dataTypeByte model.DataTypeByte
	category     model.CategoryString
}

// GetPartitionKeys returns partition keys for partitioned table append.
func (v *ValueDomain) GetPartitionKeys(partitionCol *model.Vector) ([]int, error) {
	pdt := getVectorRealDataType(partitionCol)
	if v.category != model.GetCategory(pdt) {
		return nil, errors.New("data category incompatible")
	}

	if v.category == model.TEMPORAL && v.dataTypeByte != pdt {
		df, err := model.CastDateTime(partitionCol, v.dataTypeByte)
		if err != nil {
			return nil, fmt.Errorf("can't convert type from %s to %s", model.GetDataTypeString(pdt), model.GetDataTypeString(v.dataTypeByte))
		}

		partitionCol = df.(*model.Vector)
	}

	if v.dataTypeByte == model.DtLong {
		return nil, errors.New("the partitioning column cannot be of long type")
	}

	row := partitionCol.Rows()
	res := make([]int, row)
	for i := 0; i < row; i++ {
		res[i] = partitionCol.Data.Get(i).HashBucket(1048576)
	}

	return res, nil
}
