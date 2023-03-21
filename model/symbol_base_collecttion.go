package model

import (
	"errors"

	"github.com/dolphindb/api-go/dialer/protocol"
)

type symbolBaseCollection struct {
	symBaseMap    map[uint32]*DataTypeExtend
	existingBases map[*DataTypeExtend]uint32
}

func (sbc *symbolBaseCollection) add(rd protocol.Reader, bo protocol.ByteOrder) (*DataTypeExtend, error) {
	if sbc.symBaseMap == nil {
		sbc.symBaseMap = make(map[uint32]*DataTypeExtend)
	}
	id, size, err := read2Uint32(rd, bo)
	if err != nil {
		return nil, err
	}
	if b, ok := sbc.symBaseMap[id]; ok {
		if size != 0 {
			return nil, errors.New("Invalid symbol base.")
		}

		return b, nil
	}

	base, err := readList(rd, DtString, bo, int(size))
	if err != nil {
		return nil, err
	}

	dte := &DataTypeExtend{
		BaseID:   id,
		BaseSize: size,
		Base:     base,
	}

	sbc.symBaseMap[id] = dte
	return dte, nil
}

func (sbc *symbolBaseCollection) write(wr *protocol.Writer, bo protocol.ByteOrder, base *DataTypeExtend) error {
	existing := false
	var id uint32
	if sbc.existingBases == nil {
		sbc.existingBases = map[*DataTypeExtend]uint32{
			base: 0,
		}
	} else {
		if curId, ok := sbc.existingBases[base]; ok {
			existing = true
			id = curId
		} else {
			id = uint32(len(sbc.existingBases))
			sbc.existingBases[base] = id
		}
	}

	buf := make([]byte, protocol.Uint64Size)
	bo.PutUint32(buf[0:4], id)
	if existing || base.Base == nil || base.Base.Len() == 0 {
		bo.PutUint32(buf[4:8], 0)
		return wr.Write(buf)
	}

	bo.PutUint32(buf[4:8], uint32(base.Base.Len()))
	wr.Write(buf)
	return base.Base.Render(wr, bo)
}
