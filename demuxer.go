package streammux

import (
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Handler is an interface which must be implemented by user message handlers
type Handler interface {
	Handle([]byte) ([]byte, error)
}

type requestSocket struct {
	socket
	timeout  *time.Timer
	sockID   uint16
	initSock bool
}

// Demuxer handles incoming messages dispatching them to their handler.
type Demuxer struct {
	// Reader from which demuxer reads incoming messages. Defaults to os.Stdin
	Reader io.Reader
	// Writer to which respones are written. Defaults to os.Stdout
	Writer     io.Writer
	writerLock sync.Mutex
	// WriteTimeout a timeout after which an attempt to write is aborted
	WriteTimeout time.Duration
	// Handler is a user message handler
	Handler Handler

	counter counter

	mux         []requestSocket
	concurrency uint16
	muxLock     sync.Mutex
	muxerState  uint32

	wg sync.WaitGroup
}

func (d *Demuxer) writeTimeout() time.Duration {
	timeout := d.WriteTimeout
	if timeout == 0 {
		timeout = time.Second * 30
	}
	return timeout
}

// first read which does an additional setup of internal sate of demuxer
func (d *Demuxer) initRead(r io.Reader) (*requestSocket, []byte, error) {
	d.muxLock.Lock()
	defer d.muxLock.Unlock()
	s := socket{
		headerBuf: make([]byte, HeaderSize),
	}
	b, err := s.read(r)
	if d.muxerState == 0 {
		defer atomic.StoreUint32(&d.muxerState, 1)
		timeout := d.writeTimeout()
		d.concurrency = s.header.idcap
		d.mux = make([]requestSocket, d.concurrency)
		for i := 0; i < int(d.concurrency); i++ {
			d.mux[i] = requestSocket{
				socket:  newSocket(d.concurrency),
				timeout: time.NewTimer(timeout),
			}
		}
	}
	return &requestSocket{
		socket:   s,
		initSock: true,
	}, b, err
}

func (d *Demuxer) setErr(errCh chan error, err error) {
	d.muxLock.Lock()
	defer d.muxLock.Unlock()
	if d.muxerState < 2 {
		defer atomic.StoreUint32(&d.muxerState, 2)
		errCh <- err
	}
}
func (d *Demuxer) read(r io.Reader) (*requestSocket, []byte, error) {
	var b []byte
	var err error
	var s *requestSocket
	switch atomic.LoadUint32(&d.muxerState) {
	case 0:
		s, b, err = d.initRead(r)
	case 1:
		sockID := d.counter.nextVal(d.concurrency)
		s = &d.mux[sockID]
		s.sockID = sockID
		b, err = s.read(r)
	case 2:
	}
	return s, b, err
}

func (d *Demuxer) do(errCh chan error) {
	defer d.wg.Done()
	r := d.Reader
	if r == nil {
		r = os.Stdin
	}
	w := d.Writer
	if w == nil {
		w = os.Stdout
	}
	h := d.Handler
	sock, body, err := d.read(r)
	if err == nil && sock != nil {
		d.wg.Add(1)
		go d.do(errCh)
		// reset flags
		sock.header.flags = 0
		body, err = h.Handle(body)
		if err != nil {
			body = []byte(err.Error())
			sock.header.flags |= errorFlag
			err = nil
		}
		err = sock.write(w, &d.writerLock, bodyHandler{
			b: body,
		})
		if err == nil && !sock.initSock {
			d.counter.release(sock.sockID)
		}
	}
	if err != nil {
		d.setErr(errCh, err)
	}
}

// Listen starts listening for messages
func (d *Demuxer) Listen() error {
	errCh := make(chan error, 1)
	d.wg.Add(1)
	go d.do(errCh)
	// Wait until Reader stops transmiting, for example because of io.EOF
	err := <-errCh
	// Gracefully wait 10 seconds before for all pending operations before forcefully quitting
	doneCh := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(doneCh)
	}()
	timer := time.NewTimer(time.Second * 10)
	select {
	case <-timer.C:
	case <-doneCh:
	}
	return err
}
