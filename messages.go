package streammux

import (
	"encoding/binary"
	"errors"
)

// ByteOrder represents a byte order used in communication
var ByteOrder = binary.LittleEndian

// HeaderSize represents a size of a header sent through pipe
const HeaderSize = 16

type messageFlags uint16

const (
	errorFlag messageFlags = 1 << iota
	lastFlag
)

func (m messageFlags) isError() bool {
	return m&errorFlag != 0
}

func (m messageFlags) isLast() bool {
	return m&lastFlag != 0
}

type messageHeader struct {
	id        uint16
	flags     messageFlags
	size      uint32
	idcap     uint16
	_future16 uint16
	_future   uint32
}

var (
	headerSize = binary.Size(messageHeader{})
)

func (m messageHeader) marshal(buf *[]byte) {
	ByteOrder.PutUint16((*buf)[:2], m.id)
	ByteOrder.PutUint16((*buf)[2:4], uint16(m.flags))
	ByteOrder.PutUint32((*buf)[4:8], m.size)
	ByteOrder.PutUint16((*buf)[8:10], m.idcap)
}

func (m messageHeader) err() error {
	var err error
	if m.flags.isError() {
		err = ErrorMessage{error: errors.New("error response from client")}
	}
	return err
}

func (m *messageHeader) unmarshal(b *[]byte) {
	buf := *b
	var mh messageHeader
	mh.id = ByteOrder.Uint16(buf[:2])
	mh.flags = messageFlags(ByteOrder.Uint16(buf[2:4]))
	mh.size = ByteOrder.Uint32(buf[4:8])
	mh.idcap = ByteOrder.Uint16(buf[8:10])
	*m = mh
}

type messageTuple struct {
	id   uint16
	body []byte
}
