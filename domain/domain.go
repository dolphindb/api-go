package domain

import (
	"fmt"

	"github.com/dolphindb/api-go/model"
)

// PartitionType decides how to append partitioned table.
type PartitionType string

const (
	// SEQ s the string type of PartitionType SEQ.
	SEQ PartitionType = "SEQ"
	// VALUE s the string type of PartitionType VALUE.
	VALUE PartitionType = "VALUE"
	// RANGE s the string type of PartitionType RANGE.
	RANGE PartitionType = "RANGE"
	// LIST s the string type of PartitionType LIST.
	LIST PartitionType = "LIST"
	// COMPO s the string type of PartitionType COMPO.
	COMPO PartitionType = "COMPO"
	// HASH s the string type of PartitionType HASH.
	HASH PartitionType = "HASH"
)

// Domain interface declares functions to get partition keys.
type Domain interface {
	// GetPartitionKeys returns partition keys for partitioned table append
	GetPartitionKeys(partitionCol *model.Vector) ([]int, error)
}

// CreateDomain inits a Domain according to the pt.
func CreateDomain(p PartitionType, d model.DataTypeByte, schema model.DataForm) (Domain, error) {
	switch p {
	case HASH:
		dataCat := model.GetCategory(d)
		s := schema.(*model.Scalar)
		val := s.DataType.Value()

		return &HashDomain{
			dataTypeByte: d,
			category:     dataCat,
			buckets:      int(val.(int32)),
		}, nil
	case VALUE:
		vct := schema.(*model.Vector)
		return &ValueDomain{
			dataTypeByte: vct.GetDataType(),
			category:     model.GetCategory(vct.GetDataType()),
		}, nil
	case RANGE:
		vct := schema.(*model.Vector)
		return &RangeDomain{
			dataTypeByte: vct.GetDataType(),
			category:     model.GetCategory(vct.GetDataType()),
			rangeVector:  vct,
		}, nil
	case LIST:
		vct := schema.(*model.Vector)
		if vct.GetDataType() == model.DtAny {
			d = vct.Data.ElementValue(0).(model.DataForm).GetDataType()
		} else {
			d = vct.GetDataType()
		}

		return NewListDomain(vct, d, model.GetCategory(d))
	}

	return nil, fmt.Errorf("unsupported partition type %s", p)
}

// GetPartitionType returns the string format of PartitionType with the ind.
// You can get the ind when you run schema(<TableName>).
func GetPartitionType(ind int) PartitionType {
	switch ind {
	case 0:
		return SEQ
	case 1:
		return VALUE
	case 2:
		return RANGE
	case 3:
		return LIST
	case 4:
		return COMPO
	case 5:
		return HASH
	default:
		return SEQ
	}
}

func getVectorRealDataType(vct *model.Vector) model.DataTypeByte {
	dt := vct.GetDataType()
	if dt == model.DtAny {
		dt = vct.Data.ElementValue(0).(model.DataForm).GetDataType()
	}

	return dt
}
