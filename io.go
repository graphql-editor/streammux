package streammux

import (
	"errors"
	"io"
	"math"
	"sync"
)

type bodyHandler struct {
	b       []byte
	src     io.Reader
	srcSize uint32
}

func (b bodyHandler) write(w io.Writer) (int, error) {
	if len(b.b) > 0 {
		return w.Write(b.b)
	}
	n, err := io.CopyN(w, b.src, int64(b.size()))
	if n > math.MaxInt {
		return math.MaxInt, errors.New("too much data written")
	}
	return int(n), err
}

func (b bodyHandler) size() uint32 {
	if len(b.b) > 0 {
		return uint32(len(b.b))
	}
	return b.srcSize
}

type socket struct {
	headerBuf []byte
	header    messageHeader
}

func (s *socket) checkErr(err error, b []byte) error {
	if err == nil {
		err = s.header.err(b)
	}
	return err
}

func (s *socket) read(r io.Reader) ([]byte, error) {
	var body []byte
	_, err := io.ReadFull(r, s.headerBuf)
	if err == nil {
		s.header.unmarshal(&s.headerBuf)
		body = make([]byte, s.header.size)
		_, err = io.ReadFull(r, body)
	}
	return body, s.checkErr(err, body)
}

func (s *socket) lockedRead(r io.Reader, l *sync.Mutex) ([]byte, error) {
	var body []byte
	l.Lock()
	_, err := io.ReadFull(r, s.headerBuf)
	if err == nil {
		s.header.unmarshal(&s.headerBuf)
		body = make([]byte, s.header.size)
		_, err = io.ReadFull(r, body)
	}
	l.Unlock()
	return body, s.checkErr(err, body)
}

func (s *socket) write(w io.Writer, lock *sync.Mutex, bh bodyHandler) error {
	var err error
	s.header.size = bh.size()
	s.header.marshal(&s.headerBuf)
	lock.Lock()
	if _, err = w.Write(s.headerBuf); err == nil {
		_, err = bh.write(w)
	}
	lock.Unlock()
	return err
}

func newSocket(concurrency uint16) socket {
	return socket{
		headerBuf: make([]byte, HeaderSize),
		header: messageHeader{
			idcap: concurrency,
		},
	}
}
