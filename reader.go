package mydump

import (
	"csvReader/worker"
	"io"
	"strings"
)

type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

// PooledReader is a throttled reader wrapper, where Read() calls have an upper limit of concurrency
// imposed by the given worker pool.
type PooledReader struct {
	reader    ReadSeekCloser
	ioWorkers *worker.Pool
}

// MakePooledReader constructs a new PooledReader.
func MakePooledReader(reader ReadSeekCloser, ioWorkers *worker.Pool) PooledReader {
	return PooledReader{
		reader:    reader,
		ioWorkers: ioWorkers,
	}
}

// Read implements io.Reader
func (pr PooledReader) Read(p []byte) (n int, err error) {
	if pr.ioWorkers != nil {
		w := pr.ioWorkers.Apply()
		defer pr.ioWorkers.Recycle(w)
	}
	return pr.reader.Read(p)
}

// Seek implements io.Seeker
func (pr PooledReader) Seek(offset int64, whence int) (int64, error) {
	if pr.ioWorkers != nil {
		w := pr.ioWorkers.Apply()
		defer pr.ioWorkers.Recycle(w)
	}
	return pr.reader.Seek(offset, whence)
}

// Close implements io.Closer
func (pr PooledReader) Close() error {
	return pr.reader.Close()
}

// ReadFull is same as `io.ReadFull(pr)` with less worker recycling
func (pr PooledReader) ReadFull(buf []byte) (n int, err error) {
	if pr.ioWorkers != nil {
		w := pr.ioWorkers.Apply()
		defer pr.ioWorkers.Recycle(w)
	}
	return io.ReadFull(pr.reader, buf)
}

// StringReader is a wrapper around *strings.Reader with an additional Close() method
type StringReader struct{ *strings.Reader }

// NewStringReader constructs a new StringReader
func NewStringReader(s string) StringReader {
	return StringReader{Reader: strings.NewReader(s)}
}

// Close implements io.Closer
func (StringReader) Close() error {
	return nil
}
