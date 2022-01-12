package streammux

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Handler is an interface which must be implemented by user message handlers
type Handler interface {
	Handle(io.Reader) (io.Reader, error)
}

type requestSocket struct {
	socket
	sockID      uint16
	initSock    bool
	requestBody bytes.Buffer
	dst         io.Writer
	lr          io.LimitedReader
}

// Demuxer handles incoming messages dispatching them to their handler.
//
// It is a responsibility of Muxer to make sure that there are no concurrent
// messages with the same id until all read and write operations are done for id.
type Demuxer struct {
	// BodyChunkSize represents how much per socket is allocated for body buffering
	BodyChunkSize int
	// BodyBufferSize represents maximum space that can be retained by body buffers, defaults to 256KB
	BodyBufferSize int
	// Reader from which demuxer reads incoming messages. Defaults to os.Stdin
	Reader        io.Reader
	readHeaderBuf []byte
	// Writer to which respones are written. Defaults to os.Stdout
	Writer         io.Writer
	writeHeaderBuf []byte
	writerLock     sync.Mutex
	// Handler is a user message handler
	Handler Handler

	counter counter

	mux         []requestSocket
	concurrency uint16
	muxLock     sync.Mutex
	muxerState  uint32

	wg       sync.WaitGroup
	nextRead uint32
	errCh    chan error
}

func (d *Demuxer) getDst(h messageHeader) io.Writer {
	if d.mux[0].initSock {
		d.mux[0].initSock = false
		d.concurrency = h.idcap
		d.mux = append(d.mux, make([]requestSocket, d.concurrency)...)
		for i := 1; i < int(d.concurrency); i++ {
			d.mux[i] = requestSocket{
				socket: newSocket(d.concurrency, d.BodyChunkSize),
			}
		}
	}
	return &d.mux[h.id-1].requestBody
}

func (d *Demuxer) getLimitedReader(h messageHeader) *io.LimitedReader {
	return &d.mux[h.id-1].lr
}

func (d *Demuxer) nextFree() *requestSocket {
	var s *requestSocket
	sockID := d.counter.nextVal(d.concurrency)
	s = &d.mux[sockID]
	s.sockID = sockID
	return s
}

// first read which does an additional setup of internal sate of demuxer
func (d *Demuxer) initRead(s *socket, src io.Reader) error {
	d.muxLock.Lock()
	defer d.muxLock.Unlock()
	switch d.muxerState {
	case 0:
		defer atomic.StoreUint32(&d.muxerState, 1)
		d.readHeaderBuf = make([]byte, HeaderSize)
		d.writeHeaderBuf = make([]byte, HeaderSize)
		d.mux = []requestSocket{
			{
				socket:   newSocket(0, d.BodyChunkSize),
				initSock: true,
			},
		}
	}
	return s.read(readContext{d: d}, src, &d.readHeaderBuf)
}

func (d *Demuxer) setErr(err error) {
	d.muxLock.Lock()
	defer d.muxLock.Unlock()
	if d.muxerState < 2 {
		defer atomic.StoreUint32(&d.muxerState, 2)
		d.errCh <- err
	}
}
func (d *Demuxer) read(s *socket, src io.Reader) error {
	var err error
	switch atomic.LoadUint32(&d.muxerState) {
	case 0:
		err = d.initRead(s, src)
	case 1:
		err = s.read(readContext{d: d}, src, &d.readHeaderBuf)
	}
	return err
}

func demuxerHandle(d *Demuxer) error {
	r := d.Reader
	if r == nil {
		r = os.Stdin
	}
	w := d.Writer
	if w == nil {
		w = os.Stdout
	}
	h := d.Handler
	s := socket{
		bodyChunkSize: chunkSize(d.BodyChunkSize),
	}
	err := d.read(&s, r)
	if err == nil && s.header.id != 0 {
		// Socket contains id of first fully read socket
		// continue handling using that socket and start
		// read on new go routine
		d.mux[s.header.id-1].socket.header = s.header
		sock := &d.mux[s.header.id-1]
		d.wg.Add(1)
		go d.do()
		// reset flags
		sock.header.flags = 0
		r, err := h.Handle(&sock.requestBody)
		if err != nil {
			r = bytes.NewReader([]byte(err.Error()))
			sock.header.flags |= errorFlag
			err = nil
		}
		bufSize := d.BodyBufferSize
		if bufSize == 0 {
			bufSize = 1 << 18
		}
		if sock.requestBody.Cap() > bufSize {
			sock.requestBody = *bytes.NewBuffer(make([]byte, bufSize))
		}
		sock.requestBody.Reset()
		err = sock.write(r, w, &d.writeHeaderBuf, &d.writerLock)
	}
	return err
}

func (d *Demuxer) do() {
	defer d.wg.Done()
	if err := demuxerHandle(d); err != nil {
		d.setErr(err)
	}
}

func waitOnGroup(doneCh chan struct{}, wg *sync.WaitGroup) {
	wg.Wait()
	close(doneCh)
}

// Listen starts listening for messages
func (d *Demuxer) Listen() error {
	if d.errCh != nil {
		return errors.New("already listening")
	}
	d.errCh = make(chan error, 1)
	d.wg.Add(1)
	go d.do()
	// Wait until Reader stops transmiting, for example because of io.EOF
	// Gracefully wait 10 seconds before for all pending operations before forcefully quitting
	doneCh := make(chan struct{})
	err := <-d.errCh
	go waitOnGroup(doneCh, &d.wg)
	timer := time.NewTimer(time.Second * 10)
	select {
	case <-timer.C:
	case <-doneCh:
	}
	return err
}
