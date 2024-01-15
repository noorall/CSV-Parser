package mydump

import (
	"bytes"
	"csvReader/types"
	"csvReader/worker"
	"csvReader/zeropool"
	"errors"
	"github.com/spkg/bom"
	"io"
	"regexp"
	"strings"
)

var (
	// BufferSizeScale is the factor of block buffer size
	BufferSizeScale = int64(5)
)

// Parser provides some methods to parse a source data file.
type Parser interface {
	// Pos returns means the position that parser have already handled. It's mainly used for checkpoint.
	// For normal files it's the file offset we handled.
	// For parquet files it's the row count we handled.
	// For compressed files it's the uncompressed file offset we handled.
	// TODO: replace pos with a new structure to specify position offset and rows offset
	Pos() (pos int64, rowID int64)
	SetPos(pos int64, rowID int64) error
	// ScannedPos always returns the current file reader pointer's location
	ScannedPos() (int64, error)
	Close() error
	ReadRow() error
	LastRow() Row
	RecycleRow(row Row)

	// Columns returns the _lower-case_ column names corresponding to values in
	// the LastRow.
	Columns() []string
	// SetColumns set restored column names to parser
	SetColumns([]string)

	SetRowID(rowID int64)
}

type blockParser struct {
	// states for the lexer
	reader PooledReader
	// stores data that has NOT been parsed yet, it shares same memory as appendBuf.
	buf []byte
	// used to read data from the reader, the data will be moved to other buffers.
	blockBuf    []byte
	isLastChunk bool

	// The list of column names of the last INSERT statement.
	columns []string

	rowPool *zeropool.Pool[[]types.Datum]
	lastRow Row
	// the reader position we have parsed, if the underlying reader is not
	// a compressed file, it's the file position we have parsed too.
	// this value may go backward when failed to read quoted field, but it's
	// for printing error message, and the parser should not be used later,
	// so it's ok, see readQuotedField.
	pos int64

	// cache
	remainBuf *bytes.Buffer
	appendBuf *bytes.Buffer
}

func makeBlockParser(
	reader ReadSeekCloser,
	blockBufSize int64,
	ioWorkers *worker.Pool,
) blockParser {
	pool := zeropool.New[[]types.Datum](func() []types.Datum {
		return make([]types.Datum, 0, 16)
	})
	return blockParser{
		reader:    MakePooledReader(reader, ioWorkers),
		blockBuf:  make([]byte, blockBufSize*BufferSizeScale),
		remainBuf: &bytes.Buffer{},
		appendBuf: &bytes.Buffer{},
		rowPool:   &pool,
	}
}

func (parser *blockParser) acquireDatumSlice() []types.Datum {
	return parser.rowPool.Get()
}

func unescape(
	input string,
	delim string,
	escFlavor escapeFlavor,
	escChar byte,
	unescapeRegexp *regexp.Regexp,
) string {
	if len(delim) > 0 {
		delim2 := delim + delim
		if strings.Contains(input, delim2) {
			input = strings.ReplaceAll(input, delim2, delim)
		}
	}
	if escFlavor != escapeFlavorNone && strings.IndexByte(input, escChar) != -1 {
		input = unescapeRegexp.ReplaceAllStringFunc(input, func(substr string) string {
			switch substr[1] {
			case '0':
				return "\x00"
			case 'b':
				return "\b"
			case 'n':
				return "\n"
			case 'r':
				return "\r"
			case 't':
				return "\t"
			case 'Z':
				return "\x1a"
			default:
				return substr[1:]
			}
		})
	}
	return input
}

func (parser *blockParser) readBlock() error {
	n, err := parser.reader.ReadFull(parser.blockBuf)

	switch err {
	case io.ErrUnexpectedEOF, io.EOF:
		parser.isLastChunk = true
		fallthrough
	case nil:
		// `parser.buf` reference to `appendBuf.Bytes`, so should use remainBuf to
		// hold the `parser.buf` rest data to prevent slice overlap
		parser.remainBuf.Reset()
		parser.remainBuf.Write(parser.buf)
		parser.appendBuf.Reset()
		parser.appendBuf.Write(parser.remainBuf.Bytes())
		blockData := parser.blockBuf[:n]
		if parser.pos == 0 {
			bomCleanedData := bom.Clean(blockData)
			parser.pos += int64(n - len(bomCleanedData))
			blockData = bomCleanedData
		}
		parser.appendBuf.Write(blockData)
		parser.buf = parser.appendBuf.Bytes()
		return nil
	default:
		return err
	}
}

// Pos returns the current file offset.
// Attention: for compressed sql/csv files, pos is the position in uncompressed files
func (parser *blockParser) Pos() (pos int64, lastRowID int64) {
	return parser.pos, parser.lastRow.RowID
}

// SetPos changes the reported position and row ID.
func (parser *blockParser) SetPos(pos int64, rowID int64) error {
	p, err := parser.reader.Seek(pos, io.SeekStart)
	if err != nil {
		return err
	}
	if p != pos {
		return errors.New("set pos failed, required position: %d, got: %d")
	}
	parser.pos = pos
	parser.lastRow.RowID = rowID
	return nil
}

// ScannedPos gets the read position of current reader.
// this always returns the position of the underlying file, either compressed or not.
func (parser *blockParser) ScannedPos() (int64, error) {
	return parser.reader.Seek(0, io.SeekCurrent)
}

func (parser *blockParser) Close() error {
	return parser.reader.Close()
}

// LastRow is the copy of the row parsed by the last call to ReadRow().
func (parser *blockParser) LastRow() Row {
	return parser.lastRow
}

// RecycleRow places the row object back into the allocation pool.
func (parser *blockParser) RecycleRow(row Row) {
	// We need farther benchmarking to make sure whether send a pointer
	// (instead of a slice) here can improve performance.
	parser.rowPool.Put(row.Row[:0])
}

func (parser *blockParser) Columns() []string {
	return parser.columns
}

func (parser *blockParser) SetColumns(columns []string) {
	parser.columns = columns
}

// SetRowID changes the reported row ID when we firstly read compressed files.
func (parser *blockParser) SetRowID(rowID int64) {
	parser.lastRow.RowID = rowID
}
