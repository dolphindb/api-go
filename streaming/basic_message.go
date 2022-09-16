package streaming

import (
	"strings"

	"github.com/dolphindb/api-go/model"
)

// IMessage is the interface of subscription messages.
type IMessage interface {
	// GetTopic returns the topic in string format.
	GetTopic() string
	// GetOffset returns the offset of the subscription messages.
	GetOffset() int64
	// GetValue returns the value of the subscription messages based on the column index of the subscribed table.
	GetValue(index int) model.DataForm
	// GetValueByName returns the value of the subscription messages based on the column name of the subscribed table.
	GetValueByName(name string) model.DataForm
}

// Message is the implementation of the IMessage.
type Message struct {
	offset      int64
	topic       string
	nameToIndex map[string]int

	msg *model.Vector
}

// GetTopic returns the topic in string format.
func (m *Message) GetTopic() string {
	return m.topic
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
