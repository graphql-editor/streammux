package streammux

import (
	"bytes"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

type responseSocket struct {
	socket
	errCh  chan error
	werrCh chan error
	dst    io.Writer
	lr     io.LimitedReader
}

type responseSockets []responseSocket

// Muxer is a simple multiplexer that allows raw binary transport over a pair of reader and writer.
// It implements internal bookkeeping to track response to user request
type Muxer struct {
	// BodyChunkSize represents how much per socket is allocated for body buffering
	BodyChunkSize int
	// MaxConcurrent represents a maximum in progress messages. Defaults to 256.
	MaxConcurrent uint16
	// Reader that is a source of data for multiplexer.
	Reader        io.Reader
	readHeaderBuf []byte
	readerLock    sync.Mutex

	// Writer that is a target for data for multiplexer.
	Writer         io.Writer
	writeHeaderBuf []byte
	writeBodyBuf   []byte
	writerLock     sync.Mutex

	counter counter

	mux        responseSockets
	muxLock    sync.Mutex
	muxerState uint32
	err        error
}

func (m *Muxer) getDst(h messageHeader) io.Writer {
	return m.socket(h.id).dst
}

func (m *Muxer) getLimitedReader(h messageHeader) *io.LimitedReader {
	return &m.socket(h.id).lr
}

func (m *Muxer) socket(id uint16) *responseSocket {
	return &m.mux[id-1]
}

func emitResponse(socket *responseSocket, err error) {
	socket.errCh <- err
}

func (m *Muxer) validID(id uint16) bool {
	var inUse bool
	if id >= 1 && id < m.concurrency() {
		idx, bit := m.counter.idxAndBit(id)
		_, inUse = m.counter.inUse(idx, bit)
	}
	return inUse
}

func (m *Muxer) readMessage(id uint16) error {
	reader := m.Reader
	if reader == nil {
		reader = os.Stdin
	}
	socket := m.socket(id)
	err := socket.lockedRead(readContext{m: m}, reader, &m.readHeaderBuf, &m.readerLock)
	if _, ok := err.(ErrorMessage); err != nil && !ok {
		return err
	}
	if !m.validID(socket.header.id) {
		return ErrInvalidResponse
	}
	if socket.header.id == id {
		return err
	}
	respSock := m.socket(socket.header.id)
	if respSock == socket {
		return err
	}
	select {
	case respSock.errCh <- err:
	default:
		go emitResponse(respSock, err)
	}
	return <-socket.errCh
}

func (m *Muxer) concurrency() uint16 {
	concurrency := m.MaxConcurrent
	if concurrency == 0 {
		concurrency = 1 << 8
	}
	return concurrency
}

func (m *Muxer) initMux() {
	m.muxLock.Lock()
	defer m.muxLock.Unlock()
	if m.muxerState == 0 {
		defer atomic.StoreUint32(&m.muxerState, 1)
		m.readHeaderBuf = make([]byte, HeaderSize)
		m.writeHeaderBuf = make([]byte, HeaderSize)
		m.counter.signal = make(chan struct{})
		concurrency := m.concurrency()
		m.mux = make([]responseSocket, concurrency)
		for i := 0; i < int(concurrency)-1; i++ {
			m.mux[i] = responseSocket{
				socket: newSocket(concurrency, m.BodyChunkSize),
				errCh:  make(chan error, 1),
				werrCh: make(chan error, 1),
			}
		}
	}
}

func (m *Muxer) setErr(err error) {
	m.muxLock.Lock()
	defer m.muxLock.Unlock()
	if m.muxerState < 2 {
		defer atomic.StoreUint32(&m.muxerState, 2)
		m.err = err
	}
}

func (m *Muxer) tryInit() error {
	var err error
	switch atomic.LoadUint32(&m.muxerState) {
	case 0:
		m.initMux()
		fallthrough
	case 1:
	case 2:
		err = m.err
	}
	return err
}

func (r *responseSocket) write(rd io.Reader, w io.Writer, headerBuf *[]byte, lock *sync.Mutex) error {
	return r.socket.write(rd, w, headerBuf, lock)
}

func (m *Muxer) write(id uint16, src io.Reader) error {
	socket := m.socket(id)
	socket.header.id = id
	// reset flags
	socket.header.flags = 0
	writer := m.Writer
	if writer == nil {
		writer = os.Stdout
	}
	return socket.write(src, writer, &m.writeHeaderBuf, &m.writerLock)
}

func isErrorMessage(err error) bool {
	_, ok := err.(ErrorMessage)
	return ok
}

func (m *Muxer) checkExitError(err error) error {
	if err != nil && !isErrorMessage(err) {
		m.setErr(err)
	}
	return err
}

func (m *Muxer) do(dst io.Writer, src io.Reader) error {
	id := m.counter.nextVal(m.concurrency())
	err := m.tryInit()
	var socket *responseSocket
	if err == nil {
		socket = m.socket(id)
		socket.dst = dst
		err = m.write(id, src)
	}
	if err == nil {
		err = m.readMessage(id)
	}
	if err == nil || isErrorMessage(err) {
		socket.dst = nil
		m.counter.release(id)
	}
	// Muxer assumes stable connection, any error while reading or writing
	// a message is considered fatal. This includes timeouts.
	return m.checkExitError(err)
}

// DoByte performs a request over pipe and returns a response
func (m *Muxer) DoByte(req []byte) ([]byte, error) {
	r := bytes.NewReader(req)
	var body []byte
	err := m.do((*byteBuf)(&body), r)
	return body, err
}

// Do performs a request over pipe and returns a response by reading data from reader.
// This operation requires an additional buffer to be allocated so it is slower compared to DoByte and DoSize. If size of data to be read is known ahead of time, use DoSize.
func (m *Muxer) Do(dst io.Writer, src io.Reader) error {
	return m.do(dst, src)
}
