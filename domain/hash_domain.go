package domain

import (
	"errors"
	"fmt"

	"github.com/dolphindb/api-go/model"
)

// HashDomain implements the Domain interface.
// You can use it to calculate partition keys with HASH partitionType.
type HashDomain struct {
	buckets int

	dt  model.DataTypeByte
	cat model.CategoryString
}

// GetPartitionKeys returns partition keys for partitioned table append.
func (h *HashDomain) GetPartitionKeys(partitionCol *model.Vector) ([]int, error) {
	pdt := getVectorRealDataType(partitionCol)
	if h.cat != model.GetCategory(pdt) {
		return nil, errors.New("data category incompatible")
	}

	if h.cat == model.TEMPORAL && h.dt != pdt {
		df, err := model.CastDateTime(partitionCol, h.dt)
		if err != nil {
			return nil, fmt.Errorf("can't convert type from %s to %s",
				model.GetDataTypeString(pdt), model.GetDataTypeString(h.dt))
		}

		partitionCol = df.(*model.Vector)
	}

	rows := partitionCol.Rows()
	keys := make([]int, rows)
	for i := 0; i < rows; i++ {
		keys[i] = partitionCol.HashBucket(i, h.buckets)
	}

	return keys, nil
}
