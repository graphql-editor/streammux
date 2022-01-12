package streammux_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"

	"github.com/graphql-editor/streammux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var (
	staticPayload = []byte{0, 1, 2, 3, 4, 5, 6, 7}
	byteOrder     = streammux.ByteOrder
)

func getID(p []byte) uint16 {
	return byteOrder.Uint16(p[:2])
}

func setID(p []byte, id uint16) {
	byteOrder.PutUint16(p[:2], id)
}

func getFlags(p []byte) uint16 {
	return byteOrder.Uint16(p[2:4])
}

func setFlags(p []byte, flags uint16) {
	byteOrder.PutUint16(p[2:4], flags)
}

func getSize(p []byte) uint32 {
	return byteOrder.Uint32(p[4:8])
}

func setSize(p []byte, size uint32) {
	byteOrder.PutUint32(p[4:8], size)
}

func getIDcap(p []byte) uint16 {
	return byteOrder.Uint16(p[8:10])
}

func setIDCap(p []byte, idcap uint16) {
	byteOrder.PutUint16(p[8:10], idcap)
}

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
		s := getSize(p)
		if s > 0 {
			i.writeResp = &mockReaderResp{
				header: append([]byte{}, p...),
			}
			setFlags(i.writeResp.header, uint16(2))
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

func headerMatcher(header []byte, dataLen int) func([]byte) bool {
	return func(b []byte) bool {
		flags := getFlags(b)
		size := getSize(b)
		return len(b) == 16 &&
			bytes.Equal(header[:2], b[:2]) &&
			bytes.Equal(header[8:], b[8:]) &&
			(flags == 0 || flags == 2) &&
			size >= 0 && size <= uint32(dataLen)
	}
}

func TestMuxerDoByte(t *testing.T) {
	count := 2048
	maxID := 128
	var expected [][]byte
	for i := uint64(0); i < uint64(count); i++ {
		var buf [8]byte
		byteOrder.PutUint64(buf[:], i)
		expected = append(expected, buf[:])
	}
	mio := mockRW{
		pending: make(chan *mockReaderResp, 64),
	}
	// setup header calls
	for i := 1; i < maxID; i++ {
		header := make([]byte, 16)
		setID(header, uint16(i))
		setFlags(header, 2)
		setSize(header, 8)
		setIDCap(header, 128)
		mio.On("Write", mock.MatchedBy(headerMatcher(header, 8)))
		mio.On("Read", mock.MatchedBy(headerMatcher(header, 8)))
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
	n.rpayload = !n.rpayload
	if !n.rpayload {
		return len(staticPayload), nil
	}
	id := <-n.idChan
	setID(p, id)
	setFlags(p, 2)
	setSize(p, uint32(len(staticPayload)))
	return streammux.HeaderSize, nil
}

func (n *noopRW) Write(p []byte) (int, error) {
	if n.wpayload {
		n.wpayload = false
	} else {
		n.wpayload = getSize(p) > 0
		if getFlags(p)&0x2 != 0 {
			n.idChan <- getID(p)
		}
	}
	return len(p), nil
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

type byteReader []byte

func (b *byteReader) Read(p []byte) (int, error) {
	return copy(p, staticPayload), io.EOF
}

func BenchmarkDoParallel(b *testing.B) {
	rw := noopRW{
		idChan: make(chan uint16, 64),
	}
	m := streammux.Muxer{
		Reader: &rw,
		Writer: &rw,
	}
	r := (*byteReader)(&staticPayload)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Do(io.Discard, r)
		}
	})
}

type syncNoopRW struct {
	id       uint16
	wpayload bool
	rpayload bool
}

func (n *syncNoopRW) Read(p []byte) (int, error) {
	n.rpayload = !n.rpayload
	if !n.rpayload {
		return len(staticPayload), nil
	}
	setID(p, n.id)
	setFlags(p, 2)
	setSize(p, uint32(len(staticPayload)))
	return streammux.HeaderSize, nil
}

func (n *syncNoopRW) Write(p []byte) (int, error) {
	if n.wpayload {
		n.wpayload = false
	} else {
		n.wpayload = getSize(p) > 0
		if getFlags(p)&0x2 != 0 {
			n.id = getID(p)
		}
	}
	return len(p), nil
}

func BenchmarkDoSync(b *testing.B) {
	rw := syncNoopRW{}
	m := streammux.Muxer{
		Reader: &rw,
		Writer: &rw,
	}
	r := (*byteReader)(&staticPayload)
	for i := 0; i < b.N; i++ {
		m.Do(io.Discard, r)
	}
}

func ExampleMuxer_bytePipe() {
	var m streammux.Muxer
	resp, err := m.DoByte([]byte("message"))
	fmt.Println(string(resp))
	fmt.Println(err)
}
