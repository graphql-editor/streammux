package streammux

import (
	"bytes"
	"errors"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type responseSocket struct {
	socket
	respCh chan []byte
	errCh  chan error
	timer  *time.Timer
}

// Muxer is a simple multiplexer that allows raw binary transport over a pair of reader and writer.
// It implements internal bookkeeping to track response to user request
type Muxer struct {
	// MaxConcurrent represents a maximum in progress messages. Defaults to 4096.
	MaxConcurrent uint16
	// Reader that is a source of data for multiplexer.
	Reader     io.Reader
	readerLock sync.Mutex

	// Writer that is a target for data for multiplexer.
	Writer     io.Writer
	writerLock sync.Mutex

	counter counter

	mux        []responseSocket
	muxLock    sync.Mutex
	muxerState uint32
	err        error

	// ReadTimeout determines how long muxer waits until it judges the other end as
	// unresponsive while reading a message. Defaults to 30 seconds
	ReadTimeout time.Duration

	// WriteTimeout determines how long muxer waits until it judges the other end as
	// unresponsive while writing a message. Defaults to 30 seconds
	WriteTimeout time.Duration
}

func (m *Muxer) readTimeout() time.Duration {
	timeout := m.ReadTimeout
	if timeout == time.Duration(0) {
		timeout = time.Second * 30
	}
	return timeout
}

func (m *Muxer) writeTimeout() time.Duration {
	timeout := m.WriteTimeout
	if timeout == time.Duration(0) {
		timeout = time.Second * 30
	}
	return timeout
}

func (m *Muxer) socket(mux []responseSocket, id uint16) *responseSocket {
	return &mux[id-1]
}

func emitResponse(socket *responseSocket, err error, body []byte) {
	// response channels are buffered so they are garbage collected
	// if the read timed out
	if err != nil {
		socket.errCh <- err
	} else {
		socket.respCh <- body
	}
}

func resetSocketTimer(socket *responseSocket, d time.Duration) {
	// drain timeout
	select {
	case <-socket.timer.C:
	default:
	}
	socket.timer.Reset(d)
}

func (m *Muxer) validID(id uint16) bool {
	var inUse bool
	if id >= 1 && id < m.concurrency() {
		idx, bit := m.counter.idxAndBit(id)
		_, inUse = m.counter.inUse(idx, bit)
	}
	return inUse
}

func (m *Muxer) readMessage(mux []responseSocket, id uint16) ([]byte, error) {
	reader := m.Reader
	if reader == nil {
		reader = os.Stdin
	}
	socket := m.socket(mux, id)
	body, err := socket.lockedRead(reader, &m.readerLock)
	if _, ok := err.(ErrorMessage); err != nil && !ok {
		return nil, err
	}
	if !m.validID(socket.header.id) {
		return nil, ErrInvalidResponse
	}
	if socket.header.id == id {
		return body, err
	}
	go emitResponse(m.socket(mux, socket.header.id), err, body)
	resetSocketTimer(socket, m.readTimeout())
	select {
	case body := <-socket.respCh:
		return body, nil
	case err := <-socket.errCh:
		return nil, err
	case <-socket.timer.C:
		return nil, ErrMessageReadTimeout
	}

}

func (m *Muxer) concurrency() uint16 {
	concurrency := m.MaxConcurrent
	if concurrency == 0 {
		concurrency = 1 << 12
	}
	return concurrency
}

func (m *Muxer) initMux() {
	m.muxLock.Lock()
	defer m.muxLock.Unlock()
	if m.muxerState == 0 {
		defer atomic.StoreUint32(&m.muxerState, 1)
		m.counter.signal = make(chan struct{})
		concurrency := m.concurrency()
		m.mux = make([]responseSocket, concurrency)
		timeout := m.readTimeout()
		for i := 0; i < int(concurrency)-1; i++ {
			m.mux[i] = responseSocket{
				socket: newSocket(concurrency),
				respCh: make(chan []byte, 1),
				errCh:  make(chan error, 1),
				timer:  time.NewTimer(timeout),
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

func (m *Muxer) responseMux() ([]responseSocket, error) {
	var err error
	var mux []responseSocket
	switch atomic.LoadUint32(&m.muxerState) {
	case 0:
		m.initMux()
		fallthrough
	case 1:
		mux = m.mux
	case 2:
		err = m.err
	}
	return mux, err
}

func (r *responseSocket) write(w io.Writer, lock *sync.Mutex, bh bodyHandler) {
	r.errCh <- r.socket.write(w, lock, bh)
}

func (m *Muxer) write(mux []responseSocket, id uint16, bh bodyHandler) error {
	socket := m.socket(mux, id)
	socket.header.id = id
	// reset flags
	socket.header.flags = 0
	writer := m.Writer
	if writer == nil {
		writer = os.Stdout
	}
	resetSocketTimer(socket, m.writeTimeout())
	go socket.write(writer, &m.writerLock, bh)
	select {
	case err := <-socket.errCh:
		return err
	case <-socket.timer.C:
		return ErrMessageWriteTimeout
	}
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

func (m *Muxer) do(bh bodyHandler) ([]byte, error) {
	id := m.counter.nextVal(m.concurrency())
	mux, err := m.responseMux()
	if err == nil {
		err = m.write(mux, id, bh)
	}
	var body []byte
	if err == nil {
		body, err = m.readMessage(mux, id)
	}
	m.counter.release(id)
	// Muxer assumes stable connection, any error while reading or writing
	// a message is considered fatal. This includes timeouts.
	return body, m.checkExitError(err)
}

// DoByte performs a request over pipe and returns a response
func (m *Muxer) DoByte(req []byte) ([]byte, error) {
	if len(req) > math.MaxUint32 {
		return nil, errors.New("body is too big")
	}
	bh := bodyHandler{
		b: req,
	}
	return m.do(bh)
}

// DoSize performs a request over pipe and returns a response
func (m *Muxer) DoSize(src io.Reader, srcSize uint32) ([]byte, error) {
	bh := bodyHandler{
		src:     src,
		srcSize: srcSize,
	}
	return m.do(bh)
}

// Do performs a request over pipe and returns a response by reading data from reader.
// This operation requires an additional buffer to be allocated so it is slower compared to DoByte and DoSize. If size of data to be read is known ahead of time, use DoSize.
func (m *Muxer) Do(src io.Reader) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	n, err := io.Copy(buf, src)
	if err != nil {
		return nil, err
	}
	if n > math.MaxUint32 {
		return nil, errors.New("body is too big")
	}
	bh := bodyHandler{
		src:     bytes.NewReader(buf.Bytes()),
		srcSize: uint32(n),
	}
	return m.do(bh)
}
