package streammux

import (
	"encoding/binary"
	"errors"
)

// HeaderSize represents a size of a header sent through pipe
const HeaderSize = 16

type messageFlags uint16

const (
	errorFlag messageFlags = 1 << iota
)

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
	binary.LittleEndian.PutUint16((*buf)[:2], m.id)
	binary.LittleEndian.PutUint16((*buf)[2:4], uint16(m.flags))
	binary.LittleEndian.PutUint32((*buf)[4:8], m.size)
	binary.LittleEndian.PutUint16((*buf)[8:10], m.idcap)
}

func (m messageHeader) err(b []byte) error {
	var err error
	if m.flags&errorFlag != 0 {
		err = ErrorMessage{error: errors.New(string(b))}
	}
	return err
}

func (m *messageHeader) unmarshal(b *[]byte) {
	m.id = binary.LittleEndian.Uint16((*b)[:2])
	m.flags = messageFlags(binary.LittleEndian.Uint16((*b)[2:4]))
	m.size = binary.LittleEndian.Uint32((*b)[4:8])
	m.idcap = binary.LittleEndian.Uint16((*b)[8:10])
}

type messageTuple struct {
	id   uint16
	body []byte
}
