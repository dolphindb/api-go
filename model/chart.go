package model

import (
	"fmt"
	"strings"

	"github.com/dolphindb/api-go/dialer/protocol"
)

// ChartType is a map storing the mapping relationship.
var ChartType = map[string]string{
	"0": "CT_AREA",
	"1": "CT_BAR",
	"2": "CT_COLUMN",
	"3": "CT_HISTOGRAM",
	"4": "CT_LINE",
	"5": "CT_PIE",
	"6": "CT_SCATTER",
	"7": "CT_TREND",
	"8": "CT_KLINE",
	"9": "CT_STACK",
}

// Chart is a DataForm.
type Chart struct {
	category *Category
	rowCount int

	Title     DataForm
	ChartType *Scalar
	Stacking  *Scalar
	Data      *Matrix
	Extras    *Dictionary
}

// NewChart returns an object of chart according to in.
func NewChart(in map[string]DataForm) *Chart {
	ch := &Chart{
		rowCount: len(in),
		category: newCategory(byte(DfChart), byte(DtAny)),
	}

	for k, v := range in {
		switch k {
		case "title":
			ch.Title = v.(*Vector)
		case "chartType":
			ch.ChartType = v.(*Scalar)
		case "stacking":
			ch.Stacking = v.(*Scalar)
		case "data":
			ch.Data = v.(*Matrix)
		case "extras":
			ch.Extras = v.(*Dictionary)
		}
	}
	return ch
}

// GetDataForm returns the byte type of the DataForm.
func (ch *Chart) GetDataForm() DataFormByte {
	return DfChart
}

// GetDataType returns the byte type of the DataType.
func (ch *Chart) GetDataType() DataTypeByte {
	return DtAny
}

// GetTitle returns the string type of the title.
func (ch *Chart) GetTitle() string {
	if ch.Title == nil {
		return ""
	} else if ch.Title.GetDataForm() == DfScalar {
		return ch.Title.(*Scalar).DataType.String()
	}

	return ch.Title.(*Vector).Data.ElementString(0)
}

// GetChartType returns the string type of the ChartType.
func (ch *Chart) GetChartType() string {
	if ch.ChartType == nil {
		return ""
	}

	return ChartType[ch.ChartType.DataType.String()]
}

// GetXAxisName returns the XAxisName of the title.
func (ch *Chart) GetXAxisName() string {
	if ch.Title == nil || ch.Title.GetDataForm() != DfVector || ch.Title.Rows() < 2 {
		return ""
	}

	return ch.Title.(*Vector).Data.ElementString(1)
}

// GetYAxisName returns the YAxisName of the title.
func (ch *Chart) GetYAxisName() string {
	if ch.Title == nil || ch.Title.GetDataForm() != DfVector || ch.Title.Rows() < 3 {
		return ""
	}

	return ch.Title.(*Vector).Data.ElementString(2)
}

// GetDataTypeString returns the string format of the DataType.
func (ch *Chart) GetDataTypeString() string {
	return GetDataTypeString(ch.category.DataType)
}

// Rows returns the row num of the DataForm.
func (ch *Chart) Rows() int {
	return ch.rowCount
}

// Render serializes the DataForm with bo and input it into w.
func (ch *Chart) Render(w *protocol.Writer, bo protocol.ByteOrder) error {
	if err := ch.category.render(w); err != nil {
		return err
	}

	keys, values := ch.packKeysAndValues()
	kdl, err := NewDataTypeListWithRaw(DtString, keys)
	if err != nil {
		return err
	}

	kv := NewVector(kdl)
	if err = kv.Render(w, bo); err != nil {
		return err
	}

	vdl, err := NewDataTypeListWithRaw(DtAny, values)
	if err != nil {
		return err
	}

	vv := NewVector(vdl)
	if err = vv.Render(w, bo); err != nil {
		return err
	}

	return nil
}

func (ch *Chart) packKeysAndValues() ([]string, []DataForm) {
	keys := make([]string, 0)
	values := make([]DataForm, 0)
	if ch.Title != nil {
		keys = append(keys, "title")
		values = append(values, ch.Title)
	}

	if ch.ChartType != nil {
		keys = append(keys, "chartType")
		values = append(values, ch.ChartType)
	}

	if ch.Stacking != nil {
		keys = append(keys, "stacking")
		values = append(values, ch.Stacking)
	}

	if ch.Data != nil {
		keys = append(keys, "data")
		values = append(values, ch.Data)
	}

	if ch.Extras != nil {
		keys = append(keys, "extras")
		values = append(values, ch.Extras)
	}

	return keys, values
}

func (ch *Chart) String() string {
	by := strings.Builder{}
	by.WriteString("Chart({\n")
	if ch.Title != nil {
		var val interface{}
		if ch.Title.GetDataForm() == DfVector {
			val = ch.Title.(*Vector).formatString()
		} else if ch.Title.GetDataForm() == DfScalar {
			val = ch.Title.(*Scalar).DataType.String()
		}
		by.WriteString(fmt.Sprintf("  title: %v\n", val))
	}

	if ch.ChartType != nil {
		v := ch.ChartType.DataType.String()
		by.WriteString(fmt.Sprintf("  chartType: %s\n", ChartType[v]))
	}

	if ch.Stacking != nil {
		v, err := ch.Stacking.Bool()
		if err != nil {
			return ""
		}

		by.WriteString(fmt.Sprintf("  stacking: %v\n", v))
	}

	if ch.Data != nil {
		v := ch.Data.String()
		by.WriteString(fmt.Sprintf("  data: %s\n", v))
	}

	if ch.Extras != nil {
		v := ch.Extras.String()
		by.WriteString(fmt.Sprintf("  extras: %s\n", v))
	}

	by.WriteString("})")
	return by.String()
}
