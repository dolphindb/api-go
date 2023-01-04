package model

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dolphindb/api-go/dialer/protocol"
)

// Dictionary is a DataForm.
// Refer to https://www.dolphindb.com/help/DataTypesandStructures/DataForms/Dictionary.html for more details.
type Dictionary struct {
	category *Category

	Keys   *Vector
	Values *Vector
}

// NewDictionary returns an object of Dictionary according to keys and values.
// You can instantiate the Vector object by NewVector.
func NewDictionary(keys, val *Vector) *Dictionary {
	return &Dictionary{
		category: &Category{
			DataForm: DfDictionary,
			DataType: val.GetDataType(),
		},
		Keys:   keys,
		Values: val,
	}
}

// Rows returns the row num of the DataForm.
func (dict *Dictionary) Rows() int {
	return int(dict.Keys.RowCount)
}

// GetDataForm returns the byte type of the DataForm.
func (dict *Dictionary) GetDataForm() DataFormByte {
	return DfDictionary
}

// Render serializes the DataForm with bo and input it into w.
func (dict *Dictionary) Render(w *protocol.Writer, bo protocol.ByteOrder) error {
	err := dict.category.render(w)
	if err != nil {
		return err
	}

	if dict.Keys != nil {
		err = dict.Keys.Render(w, bo)
		if err != nil {
			return err
		}
	}

	if dict.Values != nil {
		err = dict.Values.Render(w, bo)
	}

	return err
}

// GetDataType returns the byte type of the DataType.
func (dict *Dictionary) GetDataType() DataTypeByte {
	return dict.category.DataType
}

// GetDataTypeString returns the string format of the DataType.
func (dict *Dictionary) GetDataTypeString() string {
	return GetDataTypeString(dict.category.DataType)
}

// Get returns the value in dictionary based on the specified key.
func (dict *Dictionary) Get(key string) (DataType, error) {
	if dict.Keys == nil || dict.Keys.Data == nil ||
		dict.Values == nil || dict.Values.Data == nil {
		return nil, errors.New("empty dictionary")
	}

	keys := dict.Keys.Data.StringList()

	ind := -1
	for k, v := range keys {
		if v == key {
			ind = k
			break
		}
	}

	if ind < 0 {
		return nil, fmt.Errorf("invalid key: %s", key)
	}

	d := dict.Values.Data.Get(ind)
	if d == nil {
		return nil, fmt.Errorf("invalid key: %s", key)
	}

	return d, nil
}

// Set sets the key and value of a dictionary.
// If a key already exists, update the value, otherwise append the key-value pair.
func (dict *Dictionary) Set(key, value DataType) {
	if dict.Keys == nil || dict.Keys.Data == nil ||
		dict.Values == nil || dict.Values.Data == nil {
		return
	}

	keyStr := key.String()
	keys := dict.Keys.Data.StringList()
	for k, v := range keys {
		if v == keyStr {
			_ = dict.Values.Data.Set(k, value)
			return
		}
	}

	dict.Keys.Data.Append(key)
	dict.Values.Data.Append(value)
}

// KeyStrings returns the string list of dictionary keys.
func (dict *Dictionary) KeyStrings() []string {
	return dict.Keys.Data.StringList()
}

func (dict *Dictionary) String() string {
	if dict.Keys == nil || dict.Keys.Data == nil ||
		dict.Values == nil || dict.Values.Data == nil {
		return ""
	}
	keyType := GetDataTypeString(dict.Keys.Data.DataType())
	valType := GetDataTypeString(dict.Values.Data.DataType())

	by := strings.Builder{}
	by.WriteString(fmt.Sprintf("dict<%s, %s>([\n", keyType, valType))

	val := dict.Keys.formatString()
	by.WriteString(fmt.Sprintf("  %s[%d]([%s]),\n", keyType, dict.Keys.RowCount, strings.Join(val, ", ")))

	val = dict.Values.formatString()
	by.WriteString(fmt.Sprintf("  %s[%d]([%s]),\n", valType, dict.Keys.RowCount, strings.Join(val, ", ")))

	by.WriteString("])")

	return by.String()
}
