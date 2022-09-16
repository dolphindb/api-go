package model

import (
	"fmt"

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

	tl.tableName, err = readDataType(r, DtString, bo)
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
	c, err := parseCategory(r)
	if err != nil {
		return nil, err
	}

	return parseVector(r, bo, c)
}

func parseVectorWithCategoryList(r protocol.Reader, bo protocol.ByteOrder, count int) ([]*Vector, error) {
	var err error
	list := make([]*Vector, count)
	for i := 0; i < count; i++ {
		list[i], err = parseVectorWithCategory(r, bo)
		if err != nil {
			return nil, err
		}
	}

	return list, nil
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

func parseArrayVector(r protocol.Reader, bo protocol.ByteOrder, dv *Vector) error {
	arrVct := make([]*ArrayVector, 0)
	dt := dv.GetDataType() - 64
	for i := 0; i < int(dv.RowCount); {
		rc, cc, err := read2Uint16(r, bo)
		if err != nil {
			return err
		}

		total := 0
		buf, err := r.ReadCertainBytes(int(rc * cc))
		if err != nil {
			return err
		}

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

		i += int(rc)
		data, err := readList(r, dt, bo, total)
		if err != nil {
			return err
		}

		arrVct = append(arrVct, &ArrayVector{
			rowCount: rc,
			unit:     cc,
			lengths:  buf,
			data:     data,
		})
	}

	dv.ArrayVector = arrVct
	return nil
}

func parseScalar(r protocol.Reader, bo protocol.ByteOrder, c *Category) (*Scalar, error) {
	var err error
	s := &Scalar{
		category: c,
	}

	s.DataType, err = readDataType(r, c.DataType, bo)
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
