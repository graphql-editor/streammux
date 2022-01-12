package streammux_test

import (
	"bytes"
	"io"
	"sync"
	"testing"

	"github.com/graphql-editor/streammux"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockWriter struct {
	mock.Mock
}

func (r *mockWriter) Write(p []byte) (int, error) {
	r.Called(p)
	return len(p), nil
}

type mockHandler struct {
	mock.Mock
}

func (m *mockHandler) Handle(r io.Reader) (io.Reader, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	called := m.Called(b)
	return called.Get(0).(io.Reader), called.Error(1)
}

func isByteBufLen(n int) func(v []byte) bool {
	return func(v []byte) bool {
		return len(v) == n
	}
}

func TestDemuxerListen(t *testing.T) {
	pr, pw := io.Pipe()
	var mw mockWriter
	var mh mockHandler
	maxID := 128
	headers := make([][]byte, maxID)
	requests := make([][]byte, maxID-1)
	for i := 0; i < maxID-1; i++ {
		headers[i] = make([]byte, 16)
		header := headers[i]
		setID(header, uint16(i+1))
		setFlags(header, 2)
		setSize(header, 2)
		setIDCap(header, 128)
		mw.On("Write", mock.MatchedBy(headerMatcher(header, 2)))
	}
	for i := uint16(0); int(i) < maxID-1; i++ {
		buf := make([]byte, 2)
		byteOrder.PutUint16(buf, i)
		requests[i] = buf
		mh.On("Handle", buf).Return(bytes.NewReader(buf), nil)
		mw.On("Write", buf).Once()
	}
	d := streammux.Demuxer{
		Reader:  pr,
		Writer:  &mw,
		Handler: &mh,
	}
	done := make(chan struct{})
	go func() {
		d.Listen()
		close(done)
	}()
	sema := make(chan struct{}, 64)
	var wg sync.WaitGroup
	wg.Add(maxID - 1)
	var wlock sync.Mutex
	for i := 0; i < maxID-1; i++ {
		sema <- struct{}{}
		go func(i int) {
			defer wg.Done()
			header := headers[i%(maxID-1)]
			req := requests[i]
			wlock.Lock()
			_, hwerr := pw.Write(header)
			_, rwerr := pw.Write(req)
			wlock.Unlock()
			require.NoError(t, hwerr)
			require.NoError(t, rwerr)
			<-sema
		}(i)
	}
	wg.Wait()
	pw.Close()
	<-done
	mw.AssertExpectations(t)
	mh.AssertExpectations(t)
}

var staticHeader = func() []byte {
	header := make([]byte, 16)
	setID(header, 1)
	setFlags(header, 2)
	setSize(header, uint32(len(staticPayload)))
	setIDCap(header, 4096)
	return header
}()

type BenchHandler struct{}

var resp = []byte("response")

func (bh BenchHandler) Handle(r io.Reader) (io.Reader, error) {
	r = bytes.NewReader(resp)
	return r, nil
}

type BenchReader struct {
	body bool
	n    int
	N    int
}

func (b *BenchReader) Read(p []byte) (int, error) {
	if b.n >= b.N {
		return 0, io.EOF
	}
	// This bench can potentially be race'y as there's no
	// check if the id is no longer in use (as in, all writes
	// were finished before id was reused). But for the sake of
	// measuring performance of actual Demuxer combined with the fact
	// that id pool is quite big this 'bug' is acceptable.
	id := getID(staticHeader)
	id = (id+1)%4095 + 1
	setID(staticHeader, id)
	buf := staticHeader
	if b.body {
		buf = staticPayload
		b.n++
	}
	b.body = !b.body
	copy(p, buf)
	return len(buf), nil
}

type BenchWriter struct{}

func (b *BenchWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func BenchmarkDemuxerListen(b *testing.B) {
	d := streammux.Demuxer{
		Reader: &BenchReader{
			N: b.N,
		},
		Writer:  &BenchWriter{},
		Handler: &BenchHandler{},
	}
	d.Listen()
}
