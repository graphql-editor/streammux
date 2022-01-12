package streammux

type byteBuf []byte

func (buf *byteBuf) Grow(n int) {
	if cap(*buf) < n {
		nbuf := make(byteBuf, 0, n)
		nbuf = append(nbuf, *buf...)
		*buf = nbuf
	}
}

func (buf byteBuf) Len() int {
	return len(buf)
}

func (buf *byteBuf) Write(p []byte) (int, error) {
	*buf = append(*buf, p...)
	return len(p), nil
}
