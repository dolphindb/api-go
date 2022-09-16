package dialer

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/dolphindb/api-go/errors"
	"github.com/dolphindb/api-go/model"
)

type responseHeader struct {
	sessionID   []byte
	objectCount int
	byteOrder   protocol.ByteOrder
}

func (c *conn) parseResponse(reader protocol.Reader) (*responseHeader, model.DataForm, error) {
	h, err := c.parseResponseHeader(reader)
	if err != nil {
		return nil, nil, err
	}

	err = c.validateResponseOK(reader)
	if err != nil {
		return nil, nil, err
	}

	di, err := c.parseResponseContent(reader, h.objectCount, h.byteOrder)
	return h, di, err
}

func (c *conn) parseResponseHeader(reader protocol.Reader) (*responseHeader, error) {
	bs, err := reader.ReadBytes(protocol.NewLine)
	if err != nil {
		return nil, err
	}

	tmp := bytes.Split(bs, []byte{protocol.EmptySpace})
	if len(tmp) < 3 {
		return nil, errors.InvalidResponseError(fmt.Sprintf("first line items count [%d] is less than 3", len(tmp)))
	}

	h := &responseHeader{}
	h.sessionID = tmp[0]
	h.objectCount, _ = strconv.Atoi(string(tmp[1]))
	h.byteOrder = protocol.GetByteOrder(tmp[2][0])

	return h, err
}

func (c *conn) validateResponseOK(reader protocol.Reader) error {
	bs, err := reader.ReadBytes(protocol.NewLine)
	if err != nil {
		return err
	}

	if !bytes.Equal(bs, protocol.RespOK) {
		return errors.ResponseNotOKError(bs)
	}

	return nil
}

func (c *conn) parseResponseContent(r protocol.Reader, objCount int, bo protocol.ByteOrder) (model.DataForm, error) {
	switch objCount {
	case 0:
		return nil, nil
	case 1:
		return model.ParseDataForm(r, bo)
	}

	return nil, nil
}
