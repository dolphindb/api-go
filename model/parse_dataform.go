package model

import (
	"fmt"
	"math/big"

	"github.com/dolphindb/api-go/dialer/protocol"
)

func parseDictionary(r protocol.Reader, bo protocol.ByteOrder, c *Category) (*Dictionary, error) {
	var err error
	dict := &Dictionary{
		category: c,
	}

	dict.Keys, err = parseVectorWithCategory(r, bo)
	if err != nil {
		return nil, err
	}

	dict.Values, err = parseVectorWithCategory(r, bo)
	if err != nil {
		return nil, err
	}

	return dict, nil
}

func parseMatrix(r protocol.Reader, bo protocol.ByteOrder, c *Category) (*Matrix, error) {
	mtx := &Matrix{
		category: c,
	}

	buf, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	if buf&0x01 == 0x01 {
		mtx.RowLabels, err = parseVectorWithCategory(r, bo)
		if err != nil {
			return nil, err
		}
	}

	if buf&0x02 == 0x02 {
		mtx.ColumnLabels, err = parseVectorWithCategory(r, bo)
		if err != nil {
			return nil, err
		}
	}

	mtx.Data, err = parseVectorWithCategory(r, bo)
	if err != nil {
		return nil, err
	}

	return mtx, nil
}

func parsePair(r protocol.Reader, bo protocol.ByteOrder, c *Category) (*Pair, error) {
	var err error
	pr := &Pair{
		category: c,
	}

	pr.Vector, err = parseVector(r, bo, c)
	if err != nil {
		return nil, err
	}

	return pr, nil
}

func parseSet(r protocol.Reader, bo protocol.ByteOrder, c *Category) (*Set, error) {
	var err error
	s := &Set{
		category: c,
	}

	s.Vector, err = parseVectorWithCategory(r, bo)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func parseTable(r protocol.Reader, bo protocol.ByteOrder, c *Category) (*Table, error) {
	var err error
	tl := &Table{
		category: c,
	}

	tl.rowCount, tl.columnCount, err = read2Uint32(r, bo)
	if err != nil {
		return nil, err
	}

	tl.tableName, err = ParseDataType(r, DtString, bo)
	if err != nil {
		return nil, err
	}

	tl.columnNames, err = readList(r, DtString, bo, int(tl.columnCount))
	if err != nil {
		return nil, err
	}

	tl.ColNames = tl.columnNames.StringList()

	tl.columnValues, err = parseVectorWithCategoryList(r, bo, int(tl.columnCount))
	if err != nil {
		return nil, err
	}

	return tl, nil
}

func parseVectorWithCategory(r protocol.Reader, bo protocol.ByteOrder) (*Vector, error) {
	c, err := parseCategory(r, bo)
	if err != nil {
		return nil, err
	}

	return parseVector(r, bo, c)
}

func parseVectorWithCategoryList(r protocol.Reader, bo protocol.ByteOrder, count int) ([]*Vector, error) {
	list := make([]*Vector, count)
	var symBase *symbolBaseCollection
	for i := 0; i < count; i++ {
		c, err := parseCategory(r, bo)
		if err != nil {
			return nil, err
		}

		if c.DataType > 128 {
			if symBase == nil {
				symBase = &symbolBaseCollection{}
			}

			list[i], err = parseSymbolExtendVector(r, bo, symBase, c)
		} else {
			list[i], err = parseVector(r, bo, c)
		}
		if err != nil {
			return nil, err
		}
	}

	return list, nil
}

func parseSymbolExtendVector(r protocol.Reader, bo protocol.ByteOrder, symBases *symbolBaseCollection, c *Category) (*Vector, error) {
	var err error
	vct := &Vector{
		category: c,
	}

	vct.RowCount, vct.ColumnCount, err = read2Uint32(r, bo)
	if err != nil {
		return nil, err
	}

	vct.Extend, err = symBases.add(r, bo)
	if err != nil {
		return nil, err
	}

	vct.Data, err = readList(r, DtInt, bo, int(vct.RowCount*vct.ColumnCount))
	if err != nil {
		return vct, err
	}

	return vct, nil
}

func parseVector(r protocol.Reader, bo protocol.ByteOrder, c *Category) (*Vector, error) {
	var err error
	vct := &Vector{
		category: c,
	}

	vct.RowCount, vct.ColumnCount, err = read2Uint32(r, bo)
	if err != nil {
		return nil, err
	}

	err = readVectorData(r, bo, vct)
	if err != nil {
		return nil, err
	}

	return vct, nil
}

func readVectorData(r protocol.Reader, bo protocol.ByteOrder, dv *Vector) error {
	var err error

	dt := dv.GetDataType()
	switch {
	case dt > 128:
		dv.Extend = new(DataTypeExtend)
		dv.Extend.BaseID, dv.Extend.BaseSize, err = read2Uint32(r, bo)
		if err != nil {
			return err
		}

		if dv.Extend.BaseSize != 0 {
			dv.Extend.Base, err = readList(r, DtString, bo, int(dv.Extend.BaseSize))
			if err != nil {
				return err
			}
		}

		dv.Data, err = readList(r, DtInt, bo, int(dv.RowCount*dv.ColumnCount))
		if err != nil {
			return err
		}
	case dt > 64:
		err = parseArrayVector(r, bo, dv)
		if err != nil {
			return err
		}
	default:
		dv.Data, err = readList(r, dt, bo, int(dv.RowCount*dv.ColumnCount))
	}

	return err
}

func ParseArrayVector(r protocol.Reader, t DataTypeByte, bo protocol.ByteOrder) (*Vector, error) {
	vct := &Vector{
		category:  &Category{DataForm: DfVector,DataType: t},
		RowCount: 1,
	}
	err := parseArrayVector(r, bo, vct)
	if err != nil {
		return nil, err
	}
	return vct, nil
}

func parseArrayVector(r protocol.Reader, bo protocol.ByteOrder, dv *Vector) error {
	var err error
	dt := dv.GetDataType() - 64
	if dt == DtDecimal32 || dt == DtDecimal64 || dt == DtDecimal128 {
		err = readDecimalArrayVector(r, dt, bo, dv)
		if err != nil {
			return err
		}
	} else {
		err = readArrayVector(r, dt, bo, dv)
		if err != nil {
			return err
		}
	}
	return nil
}

func readDecimalScale(r protocol.Reader, bo protocol.ByteOrder) (int32, error) {
	scaRaw, err := r.ReadCertainBytes(4)
	if err != nil {
		return 0, err
	}

	return int32(bo.Uint32(scaRaw)), nil
}

func readArrayVector(r protocol.Reader, dt DataTypeByte, bo protocol.ByteOrder, dv *Vector) error {
	dv.ArrayVector = make([]*ArrayVector, 0)
	for i := 0; i < int(dv.RowCount); {
		rc, cc, err := read2Uint16(r, bo)
		if err != nil {
			return err
		}

		buf, err := r.ReadCertainBytes(int(rc * cc))
		if err != nil {
			return err
		}

		total, err := countArrayVectorElem(rc, cc, buf)
		if err != nil {
			return err
		}

		data, err := readList(r, dt, bo, total)
		if err != nil {
			return err
		}

		i += int(rc)

		dv.ArrayVector = append(dv.ArrayVector, &ArrayVector{
			rowCount: rc,
			unit:     cc,
			lengths:  buf,
			data:     data,
		})
	}

	return nil
}

func readDecimalArrayVector(r protocol.Reader, dt DataTypeByte, bo protocol.ByteOrder, dv *Vector) error {
	dv.ArrayVector = make([]*ArrayVector, 0)
	scaRaw, err := r.ReadCertainBytes(4)
	if err != nil {
		return err
	}

	dv.scale = int32(bo.Uint32(scaRaw))
	for i := 0; i < int(dv.RowCount); {
		rc, cc, err := read2Uint16(r, bo)
		if err != nil {
			return err
		}

		buf, err := r.ReadCertainBytes(int(rc * cc))
		if err != nil {
			return err
		}

		total, err := countArrayVectorElem(rc, cc, buf)
		if err != nil {
			return err
		}

		data, err := readDecimal(r, dt, bo, total, dv.scale)
		if err != nil {
			return err
		}

		i += int(rc)

		dv.ArrayVector = append(dv.ArrayVector, &ArrayVector{
			rowCount: rc,
			unit:     cc,
			lengths:  buf,
			data:     data,
		})
	}

	return nil
}

func readDecimal(r protocol.Reader, t DataTypeByte, bo protocol.ByteOrder, count int, sca int32) (DataTypeList, error) {
	dt := &dataTypeList{
		t:     t,
		count: count,
		bo:    bo,
	}

	var err error
	if bo == protocol.LittleEndian {
		err = readDecimalWithLittleEndian(dt, r, t, count, sca)
	} else {
		err = readDecimalWithBigEndian(dt, r, bo, t, count, sca)
	}

	return dt, err
}

func readDecimalWithBigEndian(dt *dataTypeList, r protocol.Reader, bo protocol.ByteOrder, t DataTypeByte, count int, sca int32) error {
	switch t {
	case DtDecimal32:
		d32, err := readIntWithBigEndian(count, r, bo)
		if err != nil {
			return err
		}

		dt.decimal32Data = make([]int32, count+1)
		dt.decimal32Data[0] = sca
		copy(dt.decimal32Data[1:], d32)
	case DtDecimal64:
		d64, err := readLongsWithBigEndian(count, r, bo)
		if err != nil {
			return err
		}

		dt.decimal64Data = make([]int64, count+1)
		dt.decimal64Data[0] = int64(sca)
		copy(dt.decimal64Data[1:], d64)
	case DtDecimal128:
		d128, err := readBigIntWithBigEndian(count, r)
		if err != nil {
			return err
		}

		dt.decimal128Data = decimal128Datas{scale: sca, value: make([]*big.Int, count)}
		copy(dt.decimal128Data.value, d128)
	}

	return nil
}

func readDecimalWithLittleEndian(dt *dataTypeList, r protocol.Reader, t DataTypeByte, count int, sca int32) error {
	switch t {
	case DtDecimal32:
		d32, err := readIntWithLittleEndian(count, r)
		if err != nil {
			return err
		}

		dt.decimal32Data = make([]int32, count+1)
		dt.decimal32Data[0] = sca
		copy(dt.decimal32Data[1:], d32)
	case DtDecimal64:
		d64, err := readLongsWithLittleEndian(count, r)
		if err != nil {
			return err
		}

		dt.decimal64Data = make([]int64, count+1)
		dt.decimal64Data[0] = int64(sca)
		copy(dt.decimal64Data[1:], d64)
	case DtDecimal128:
		d128, err := readBigIntWithLittleEndian(count, r)
		if err != nil {
			return err
		}

		dt.decimal128Data = decimal128Datas{scale: sca, value: make([]*big.Int, count)}
		copy(dt.decimal128Data.value, d128)
	}

	return nil
}

func countArrayVectorElem(rc, cc uint16, buf []byte) (int, error) {
	total := 0

	switch {
	case cc == 1:
		res := protocol.Uint8SliceFromByteSlice(buf)
		for _, v := range res {
			total += int(v)
		}
	case cc == 2:
		res := protocol.Uint16SliceFromByteSlice(buf)
		for _, v := range res {
			total += int(v)
		}
	case cc == 4:
		res := protocol.Uint32SliceFromByteSlice(buf)
		for _, v := range res {
			total += int(v)
		}
	}

	return total, nil
}

func parseScalar(r protocol.Reader, bo protocol.ByteOrder, c *Category) (*Scalar, error) {
	var err error
	s := &Scalar{
		category: c,
	}

	s.DataType, err = ParseDataType(r, c.DataType, bo)
	return s, err
}

func parseChart(r protocol.Reader, bo protocol.ByteOrder, c *Category) (*Chart, error) {
	var err error
	ch := &Chart{
		category: c,
	}

	vc, err := parseVectorWithCategory(r, bo)
	if err != nil {
		return nil, err
	}

	values, err := parseVectorWithCategory(r, bo)
	if err != nil {
		return nil, err
	}

	if values.GetDataType() != DtAny {
		return nil, fmt.Errorf("invalid data")
	}

	keys := vc.Data.StringList()
	val := values.Data.Value()
	for k, v := range keys {
		df := val[k].(DataForm)
		switch v {
		case "title":
			ch.Title = df.(*Vector)
		case "chartType":
			ch.ChartType = df.(*Scalar)
		case "stacking":
			ch.Stacking = df.(*Scalar)
		case "extras":
			ch.Extras = df.(*Dictionary)
		case "data":
			ch.Data = df.(*Matrix)
		}
	}

	ch.rowCount = len(keys)

	return ch, nil
}
