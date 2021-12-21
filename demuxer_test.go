package streammux_test

import (
	"encoding/binary"
	"io"
	"sync"
	"testing"

	streammux "github.com/graphql-editor/io-pipe"
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

func (m *mockHandler) Handle(b []byte) ([]byte, error) {
	called := m.Called(b)
	return called.Get(0).([]byte), called.Error(1)
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
	count := 512
	maxID := 128
	headers := make([][]byte, maxID)
	requests := make([][]byte, count)
	for i := 0; i < maxID-1; i++ {
		headers[i] = make([]byte, 16)
		header := headers[i]
		binary.LittleEndian.PutUint16(header[:2], uint16(i+1))
		binary.LittleEndian.PutUint32(header[4:8], 2)
		binary.LittleEndian.PutUint16(header[8:10], 128)
		mw.On("Write", header)
	}
	for i := uint16(0); int(i) < count; i++ {
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, i)
		requests[i] = buf
		mh.On("Handle", buf).Return(buf, nil)
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
	wg.Add(count)
	var wlock sync.Mutex
	for i := 0; i < count; i++ {
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
	binary.LittleEndian.PutUint16(header[:2], uint16(1))
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(staticPayload)))
	binary.LittleEndian.PutUint16(header[8:10], 4096)
	return header
}()

type BenchHandler struct{}

func (bh BenchHandler) Handle(b []byte) ([]byte, error) {
	return b, nil
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
