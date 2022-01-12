package streammux

import (
	"io"
	"sync"
)

const defaultChunkSize = 1 << 13

type socket struct {
	header        messageHeader
	bodyChunkSize int
	bodyBuf       []byte
}

type readContext struct {
	m *Muxer
	d *Demuxer
}

func (r readContext) getDst(h messageHeader) io.Writer {
	if r.m != nil {
		return r.m.getDst(h)
	}
	return r.d.getDst(h)
}

func (r readContext) getLimitedReader(h messageHeader) *io.LimitedReader {
	if r.m != nil {
		return r.m.getLimitedReader(h)
	}
	return r.d.getLimitedReader(h)
}

func (s *socket) checkErr(err error) error {
	if err == nil {
		err = s.header.err()
	}
	return err
}

type growable interface {
	Grow(n int)
	Len() int
}

func (s *socket) readCritical(rc readContext, src io.Reader, headerBuf *[]byte) (bool, error) {
	_, err := io.ReadFull(src, *headerBuf)
	if err == nil {
		s.header.unmarshal(headerBuf)
		dst := rc.getDst(s.header)
		if s.header.size != 0 {
			if gw, ok := dst.(growable); ok {
				gw.Grow(gw.Len() + int(s.header.size))
			}
			var written int64
			lr := rc.getLimitedReader(s.header)
			*lr = io.LimitedReader{
				R: src,
				N: int64(s.header.size),
			}
			written, err = io.Copy(dst, lr)
			if written < int64(s.header.size) && err == nil {
				err = io.EOF
			}
		}
	}
	var done bool
	if err == nil {
		done = s.header.flags.isLast()
	}
	return done, err
}

func (s *socket) read(rc readContext, src io.Reader, headerBuf *[]byte) error {
	var done bool
	var err error
	for err == nil && !done {
		done, err = s.readCritical(rc, src, headerBuf)
	}
	return s.checkErr(err)
}

func (s *socket) lockedRead(rc readContext, src io.Reader, headerBuf *[]byte, l *sync.Mutex) error {
	var err error
	var done bool
	for err == nil && !done {
		l.Lock()
		done, err = s.readCritical(rc, src, headerBuf)
		l.Unlock()
	}
	return s.checkErr(err)
}

func (s *socket) write(r io.Reader, w io.Writer, headerBuf *[]byte, lock *sync.Mutex) error {
	var err error
	var werr error
	for err == nil && werr == nil {
		if cap(s.bodyBuf) < s.bodyChunkSize {
			s.bodyBuf = make([]byte, s.bodyChunkSize)
		}
		var n int
		n, err = r.Read(s.bodyBuf)
		lock.Lock()
		header := s.header
		switch err {
		case io.EOF:
			header.flags += lastFlag
			fallthrough
		case nil:
			if n > 0 || header.flags.isLast() {
				header.size = uint32(n)
				header.marshal(headerBuf)
				if _, werr = w.Write(*headerBuf); werr == nil {
					if n > 0 {
						_, werr = w.Write(s.bodyBuf[:n])
					}
				}
			}
		}
		lock.Unlock()
	}
	if err == io.EOF {
		err = nil
	}
	if err == nil {
		err = werr
	}
	return err
}

func newSocket(concurrency uint16, bodyChunkSize int) socket {
	return socket{
		bodyChunkSize: chunkSize(bodyChunkSize),
		header: messageHeader{
			idcap: concurrency,
		},
	}
}

func chunkSize(n int) int {
	if n < 1 {
		n = defaultChunkSize
	}
	return n
}
