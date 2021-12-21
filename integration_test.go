package streammux_test

import (
	"encoding/binary"
	"io"
	"sync"
	"testing"

	streammux "github.com/graphql-editor/io-pipe"
	"github.com/stretchr/testify/assert"
)

type integrationTestHadnler struct{}

func (i integrationTestHadnler) Handle(b []byte) ([]byte, error) {
	return b, nil
}

func TestIntegration(t *testing.T) {
	count := 512
	maxID := 128
	mr, dw := io.Pipe()
	dr, mw := io.Pipe()
	d := streammux.Demuxer{
		Reader:  dr,
		Writer:  dw,
		Handler: integrationTestHadnler{},
	}
	done := make(chan struct{})
	go func() {
		d.Listen()
		close(done)
	}()
	m := streammux.Muxer{
		Reader:        mr,
		Writer:        mw,
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
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, uint16(i))
		sema <- &results[i]
		go func(req []byte) {
			defer wg.Done()
			result := resultsData{
				request: req,
			}
			result.response, result.err = m.DoByte(req)
			resPtr := <-sema
			*resPtr = result
		}(buf)
	}
	wg.Wait()
	mw.Close()
	<-done
	assert.Len(t, results, count)
	for _, r := range results {
		assert.Equal(t, r.request, r.response)
		assert.NoError(t, r.err)
	}
}
