package streammux_test

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"

	streammux "github.com/graphql-editor/io-pipe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var (
	staticPayload = []byte{0, 1, 2, 3, 4, 5, 6, 7}
)

type mockReaderResp struct {
	header []byte
	body   []byte
}

// Very simple, unsafe on purpose, mock that for each request
// responds to it with the same payload at some point.
type mockRW struct {
	mock.Mock
	readResps []*mockReaderResp
	readResp  *mockReaderResp
	writeResp *mockReaderResp
	pending   chan *mockReaderResp
}

func (i *mockRW) resp() {
	// randomize response a bit to simulate out of order responses
	for {
		if len(i.readResps) == 0 {
			i.readResps = append(i.readResps, <-i.pending)
		}
		select {
		case r := <-i.pending:
			i.readResps = append(i.readResps, r)
		default:
			ridx := rand.Intn(len(i.readResps))
			i.readResp = i.readResps[ridx]
			i.readResps = append(i.readResps[:ridx], i.readResps[ridx+1:]...)
			return
		}
	}
}

func (i *mockRW) Read(p []byte) (int, error) {
	if i.readResp == nil {
		i.resp()
		i.Called(i.readResp.header)
		copy(p, []byte(i.readResp.header))
		return len(i.readResp.header), nil
	}
	resp := i.readResp
	i.readResp = nil
	i.Called(resp.body)
	copy(p, []byte(resp.body))
	return len(resp.body), nil
}

func (i *mockRW) Write(p []byte) (int, error) {
	if i.writeResp == nil {
		i.writeResp = &mockReaderResp{
			header: append([]byte{}, p...),
		}
		i.Called(p)
		return len(p), nil
	}
	i.Called(p)
	resp := i.writeResp
	i.writeResp = nil
	resp.body = append([]byte{}, p...)
	go func(resp *mockReaderResp) {
		i.pending <- resp
	}(resp)
	return len(p), nil
}

func TestMuxerDoByte(t *testing.T) {
	count := 2048
	maxID := 128
	var expected [][]byte
	for i := uint64(0); i < uint64(count); i++ {
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], i)
		expected = append(expected, buf[:])
	}
	mio := mockRW{
		pending: make(chan *mockReaderResp, 64),
	}
	// setup header calls
	for i := 1; i < maxID; i++ {
		header := make([]byte, 16)
		binary.LittleEndian.PutUint16(header[:2], uint16(i))
		binary.LittleEndian.PutUint32(header[4:8], 8)
		binary.LittleEndian.PutUint16(header[8:10], 128)
		mio.On("Write", header)
		mio.On("Read", header)
	}

	// setup body calls
	for i := 0; i < count; i++ {
		body := expected[i]
		mio.On("Write", body).Once()
		mio.On("Read", body).Once()
	}
	m := &streammux.Muxer{
		Reader:        &mio,
		Writer:        &mio,
		MaxConcurrent: uint16(maxID),
	}
	type resultsData struct {
		request  []byte
		response []byte
		err      error
	}
	results := make([]resultsData, count)
	sema := make(chan *resultsData, 64)
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		sema <- &results[i]
		go func(req []byte) {
			defer wg.Done()
			result := resultsData{
				request: req,
			}
			result.response, result.err = m.DoByte(req)
			resPtr := <-sema
			*resPtr = result
		}(expected[i])
	}
	wg.Wait()
	mio.AssertExpectations(t)
	assert.Len(t, results, count)
	for _, r := range results {
		assert.Len(t, r.request, 8)
		if !assert.Equal(t, r.request, r.response) {
			fmt.Println(1)
		}
		assert.Contains(t, expected, r.request)
		assert.NoError(t, r.err)
	}
}

type noopRW struct {
	rpayload bool
	wpayload bool
	idChan   chan uint16
}

func (n *noopRW) Read(p []byte) (int, error) {
	_ = p[7]
	if n.rpayload {
		n.rpayload = false
		return len(staticPayload), nil
	}
	id, ok := <-n.idChan
	if !ok {
		return 0, io.EOF
	}
	p[4] = 8
	binary.LittleEndian.PutUint16(p[:2], id)
	binary.LittleEndian.PutUint32(p[4:], uint32(len(staticPayload)))
	n.rpayload = true
	return streammux.HeaderSize, nil
}

func (n *noopRW) Write(p []byte) (int, error) {
	if n.wpayload {
		n.wpayload = false
		return len(p), nil
	}
	_ = p[7]
	id := binary.LittleEndian.Uint16(p[:2])
	n.idChan <- id
	n.wpayload = true
	return streammux.HeaderSize, nil
}

func BenchmarkMuxerDoByte(b *testing.B) {
	rw := noopRW{
		idChan: make(chan uint16, 64),
	}
	m := streammux.Muxer{
		Reader: &rw,
		Writer: &rw,
	}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.DoByte(staticPayload)
		}
	})
}

func ExampleMuxer_bytePipe() {
	var m streammux.Muxer
	resp, err := m.DoByte([]byte("message"))
	fmt.Println(string(resp))
	fmt.Println(err)
}
