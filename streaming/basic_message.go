package streaming

import (
	"strings"

	"github.com/dolphindb/api-go/model"
)

// IMessage is the interface of subscription messages.
type IMessage interface {
	// GetTopic returns the topic in string format.
	GetTopic() string
	GetSym() string
	// GetOffset returns the offset of the subscription messages.
	GetOffset() int64
	// GetValue returns the value of the subscription messages based on the column index of the subscribed table.
	GetValue(index int) model.DataForm
	// GetValueByName returns the value of the subscription messages based on the column name of the subscribed table.
	GetValueByName(name string) model.DataForm
	// Size returns the rows of the subscription messages.
	Size() int
}

// Message is the implementation of the IMessage.
type Message struct {
	offset      int64
	topic       string
	sym         string
	nameToIndex map[string]int

	msg *model.Vector
}

// GetTopic returns the topic in string format.
func (m *Message) GetTopic() string {
	return m.topic
}

// GetTopic returns the topic in string format.
func (m *Message) GetSym() string {
	return m.sym
}

// GetOffset returns the offset of the subscription messages.
func (m *Message) GetOffset() int64 {
	return m.offset
}

// GetValue returns the value of the subscription message based on the column index of the subscribed table.
func (m *Message) GetValue(index int) model.DataForm {
	if m.msg != nil {
		return m.msg.Data.ElementValue(index).(model.DataForm)
	}

	return nil
}

// GetValueByName returns the value of the subscription message based on the column name of the subscribed table.
func (m *Message) GetValueByName(name string) model.DataForm {
	if m.msg != nil {
		if ind, ok := m.nameToIndex[strings.ToLower(name)]; ok {
			return m.msg.Data.ElementValue(ind).(model.DataForm)
		}
	}

	return nil
}

// Size returns the rows of the subscription messages.
func (m *Message) Size() int {
	return m.msg.Rows()
}
type TableMessage struct {
	offset      int64
	topic       string
	sym         string

	msg *model.Table
}

// GetTopic returns the topic in string format.
func (m *TableMessage) GetTopic() string {
	return m.topic
}

// GetTopic returns the topic in string format.
func (m *TableMessage) GetSym() string {
	return m.sym
}

// GetOffset returns the offset of the subscription TableMessages.
func (m *TableMessage) GetOffset() int64 {
	return m.offset
}

// GetValue returns the value of the subscription TableMessage based on the column index of the subscribed table.
func (m *TableMessage) GetValue(index int) model.DataForm {
	if m.msg != nil {
		return m.msg.GetColumnByIndex(index)
	}

	return nil
}

// GetValueByName returns the value of the subscription TableMessage based on the column name of the subscribed table.
func (m *TableMessage) GetValueByName(name string) model.DataForm {
	if m.msg != nil {
		return m.msg.GetColumnByName(name)
	}

	return nil
}

// Size returns the rows of the subscription TableMessages.
func (m *TableMessage) Size() int {
	return m.msg.Rows()
}
