package mydump_test

import (
	mydump "csvReader"
	"csvReader/config"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

// TODO: rewrite test case
func assertPosEqual(t *testing.T, parser *mydump.CSVParser, expectPos, expectRowID int64) {
	pos, rowID := parser.Pos()
	require.Equal(t, expectPos, pos)
	require.Equal(t, expectRowID, rowID)
}

var nullDatum = newStringField("", true)

func newStringField(val string, isNull bool) mydump.Field {
	return mydump.Field{
		Val:    val,
		IsNull: isNull,
	}
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

func tpchDatums() [][]mydump.Field {
	datums := make([][]mydump.Field, 0, 3)
	datums = append(datums, []mydump.Field{
		newStringField("1", false),
		newStringField("goldenrod lavender spring chocolate lace", false),
		newStringField("Manufacturer#1", false),
		newStringField("Brand#13", false),
		newStringField("PROMO BURNISHED COPPER", false),
		newStringField("7", false),
		newStringField("JUMBO PKG", false),
		newStringField("901.00", false),
		newStringField("ly. slyly ironi", false),
	})
	datums = append(datums, []mydump.Field{
		newStringField("2", false),
		newStringField("blush thistle blue yellow saddle", false),
		newStringField("Manufacturer#1", false),
		newStringField("Brand#13", false),
		newStringField("LARGE BRUSHED BRASS", false),
		newStringField("1", false),
		newStringField("LG CASE", false),
		newStringField("902.00", false),
		newStringField("lar accounts amo", false),
	})
	datums = append(datums, []mydump.Field{
		newStringField("3", false),
		newStringField("spring green yellow purple cornsilk", false),
		newStringField("Manufacturer#4", false),
		newStringField("Brand#42", false),
		newStringField("STANDARD POLISHED BRASS", false),
		newStringField("21", false),
		newStringField("WRAP CASE", false),
		newStringField("903.00", false),
		newStringField("egular deposits hag", false),
	})

	return datums
}

func datumsToString(datums [][]mydump.Field, delimitor string, quote string, lastSep bool) string {
	var b strings.Builder
	doubleQuote := quote + quote
	for _, ds := range datums {
		for i, d := range ds {
			text := d.Val
			if len(quote) > 0 {
				b.WriteString(quote)
				b.WriteString(strings.ReplaceAll(text, quote, doubleQuote))
				b.WriteString(quote)
			} else {
				b.WriteString(text)
			}
			if lastSep || i < len(ds)-1 {
				b.WriteString(delimitor)
			}
		}
		b.WriteString("\r\n")
	}
	return b.String()
}

func TestTPCH(t *testing.T) {
	datums := tpchDatums()
	input := datumsToString(datums, ",", "", true)
	reader := strings.NewReader(input)

	cfg := config.CSVConfig{
		Separator: ",",
		Delimiter: "",
	}

	parser, err := mydump.NewCSVParser(&cfg, reader, int64(config.ReadBlockSize), false)
	require.NoError(t, err)
	require.Nil(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID:  1,
		Fields: datums[0],
		Length: 116,
	}, parser.LastRow())
	assertPosEqual(t, parser, 126, 1)

	require.Nil(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID:  2,
		Fields: datums[1],
		Length: 104,
	}, parser.LastRow())
	assertPosEqual(t, parser, 241, 2)

	require.Nil(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID:  3,
		Fields: datums[2],
		Length: 117,
	}, parser.LastRow())
	assertPosEqual(t, parser, 369, 3)

}

func TestTPCHMultiBytes(t *testing.T) {
	datums := tpchDatums()
	sepsAndQuotes := [][2]string{
		{",", ""},
		{"\000", ""},
		{"，", ""},
		{"🤔", ""},
		{"，", "。"},
		{"||", ""},
		{"|+|", ""},
		{"##", ""},
		{"，", "'"},
		{"，", `"`},
		{"🤔", `''`},
		{"🤔", `"'`},
		{"🤔", `"'`},
		{"🤔", "🌚"}, // this two emoji have same prefix bytes
		{"##", "#-"},
		{"\\s", "\\q"},
		{",", "1"},
		{",", "ac"},
	}
	for _, SepAndQuote := range sepsAndQuotes {
		inputStr := datumsToString(datums, SepAndQuote[0], SepAndQuote[1], false)

		// extract all index in the middle of '\r\n' from the inputStr.
		// they indicate where the parser stops after reading one row.
		// should be equals to the number of datums.
		var allExpectedParserPos []int
		for {
			last := 0
			if len(allExpectedParserPos) > 0 {
				last = allExpectedParserPos[len(allExpectedParserPos)-1]
			}
			pos := strings.IndexByte(inputStr[last:], '\r')
			if pos < 0 {
				break
			}
			allExpectedParserPos = append(allExpectedParserPos, last+pos+1)
		}
		require.Len(t, allExpectedParserPos, len(datums))

		cfg := config.CSVConfig{
			Separator:   SepAndQuote[0],
			Delimiter:   SepAndQuote[1],
			TrimLastSep: false,
		}

		reader := NewStringReader(inputStr)
		parser, err := mydump.NewCSVParser(&cfg, reader, int64(config.ReadBlockSize), false)
		require.NoError(t, err)

		for i, expectedParserPos := range allExpectedParserPos {
			require.Nil(t, parser.readRow())
			require.Equal(t, int64(i+1), parser.LastRow().RowID)
			require.Equal(t, datums[i], parser.LastRow().Fields)
			assertPosEqual(t, parser, int64(expectedParserPos), int64(i+1))
		}

	}
}

func TestRFC4180(t *testing.T) {
	cfg := config.CSVConfig{
		Separator: ",",
		Delimiter: `"`,
	}

	// example 1, trailing new lines

	parser, err := mydump.NewCSVParser(&cfg, NewStringReader("aaa,bbb,ccc\nzzz,yyy,xxx\n"), int64(config.ReadBlockSize), false)
	require.NoError(t, err)

	require.Nil(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Fields: []mydump.Field{
			newStringField("aaa", false),
			newStringField("bbb", false),
			newStringField("ccc", false),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 12, 1)

	require.Nil(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Fields: []mydump.Field{
			newStringField("zzz", false),
			newStringField("yyy", false),
			newStringField("xxx", false),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 24, 2)

	// example 2, no trailing new lines

	parser, err = mydump.NewCSVParser(&cfg, NewStringReader("aaa,bbb,ccc\nzzz,yyy,xxx"), int64(config.ReadBlockSize), false)
	require.NoError(t, err)

	require.Nil(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Fields: []mydump.Field{
			newStringField("aaa", false),
			newStringField("bbb", false),
			newStringField("ccc", false),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 12, 1)

	require.Nil(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Fields: []mydump.Field{
			newStringField("zzz", false),
			newStringField("yyy", false),
			newStringField("xxx", false),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 23, 2)

	// example 5, quoted fields

	parser, err = mydump.NewCSVParser(&cfg, NewStringReader(`"aaa","bbb","ccc"`+"\nzzz,yyy,xxx"), int64(config.ReadBlockSize), false)
	require.NoError(t, err)

	require.Nil(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Fields: []mydump.Field{
			newStringField("aaa", false),
			newStringField("bbb", false),
			newStringField("ccc", false),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 18, 1)

	require.Nil(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Fields: []mydump.Field{
			newStringField("zzz", false),
			newStringField("yyy", false),
			newStringField("xxx", false),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 29, 2)

	// example 6, line breaks within fields

	parser, err = mydump.NewCSVParser(&cfg, NewStringReader(`"aaa","b
bb","ccc"
zzz,yyy,xxx`), int64(config.ReadBlockSize), false)
	require.NoError(t, err)

	require.Nil(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Fields: []mydump.Field{
			newStringField("aaa", false),
			newStringField("b\nbb", false),
			newStringField("ccc", false),
		},
		Length: 10,
	}, parser.LastRow())
	assertPosEqual(t, parser, 19, 1)

	require.Nil(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Fields: []mydump.Field{
			newStringField("zzz", false),
			newStringField("yyy", false),
			newStringField("xxx", false),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 30, 2)

	// example 7, quote escaping

	parser, err = mydump.NewCSVParser(&cfg, NewStringReader(`"aaa","b""bb","ccc"`), int64(config.ReadBlockSize), false)
	require.NoError(t, err)

	require.Nil(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Fields: []mydump.Field{
			newStringField("aaa", false),
			newStringField("b\"bb", false),
			newStringField("ccc", false),
		},
		Length: 10,
	}, parser.LastRow())
	assertPosEqual(t, parser, 19, 1)

}

func TestMySQL(t *testing.T) {
	cfg := config.CSVConfig{
		Separator:  ",",
		Delimiter:  `"`,
		Terminator: "\n",
		EscapedBy:  `\`,
		NotNull:    false,
		Null:       []string{`\N`},
	}

	parser, err := mydump.NewCSVParser(&cfg, NewStringReader(`"\"","\\","\?"
"\
",\N,\\N`), int64(config.ReadBlockSize), false)
	require.NoError(t, err)

	require.NoError(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Fields: []mydump.Field{
			newStringField(`"`, false),
			newStringField(`\`, false),
			newStringField("?", false),
		},
		Length: 6,
	}, parser.LastRow())
	var lines [][]mydump.Field
	lines = append(lines, parser.LastRow().Fields)
	assertPosEqual(t, parser, 15, 1)

	require.NoError(t, parser.readRow())
	//require.Equal(t, mydump.Row{
	//	RowID: 2,
	//	Fields: []mydump.Field{
	//		newStringField("\n", false),
	//		nullDatum,
	//		newStringField(`\N`, false),
	//	},
	//	Length: 7,
	//}, parser.LastRow())
	lines = append(lines, parser.LastRow().Fields)
	assertPosEqual(t, parser, 26, 2)

	parser, err = mydump.NewCSVParser(
		&cfg,
		NewStringReader(`"\0\b\n\r\t\Z\\\  \c\'\""`),
		int64(config.ReadBlockSize), false)
	require.NoError(t, err)

	require.NoError(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Fields: []mydump.Field{
			newStringField(string([]byte{0, '\b', '\n', '\r', '\t', 26, '\\', ' ', ' ', 'c', '\'', '"'}), false),
		},
		Length: 23,
	}, parser.LastRow())
	lines = append(lines, parser.LastRow().Fields)

	cfg.UnescapedQuote = true
	parser, err = mydump.NewCSVParser(
		&cfg,
		NewStringReader(`3,"a string containing a " quote",102.20
`),
		int64(config.ReadBlockSize), false)
	require.NoError(t, err)

	require.NoError(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Fields: []mydump.Field{
			newStringField("3", false),
			newStringField(`a string containing a " quote`, false),
			newStringField("102.20", false),
		},
		Length: 36,
	}, parser.LastRow())

	parser, err = mydump.NewCSVParser(
		&cfg,
		NewStringReader(`3,"a string containing a " quote","102.20"`),
		int64(config.ReadBlockSize), false)
	require.NoError(t, err)

	require.NoError(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Fields: []mydump.Field{
			newStringField("3", false),
			newStringField(`a string containing a " quote`, false),
			newStringField("102.20", false),
		},
		Length: 36,
	}, parser.LastRow())

	parser, err = mydump.NewCSVParser(
		&cfg,
		NewStringReader(`"a"b",c"d"e`),
		int64(config.ReadBlockSize), false)
	require.NoError(t, err)

	require.NoError(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Fields: []mydump.Field{
			newStringField(`a"b`, false),
			newStringField(`c"d"e`, false),
		},
		Length: 8,
	}, parser.LastRow())
}

func TestCustomEscapeChar(t *testing.T) {
	cfg := config.CSVConfig{
		Separator: ",",
		Delimiter: `"`,
		EscapedBy: `!`,
		NotNull:   false,
		Null:      []string{`!N`},
	}

	parser, err := mydump.NewCSVParser(&cfg, NewStringReader(`"!"","!!","!\"
"!
",!N,!!N`), int64(config.ReadBlockSize), false)
	require.NoError(t, err)

	require.Nil(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Fields: []mydump.Field{
			newStringField(`"`, false),
			newStringField(`!`, false),
			newStringField(`\`, false),
		},
		Length: 6,
	}, parser.LastRow())
	assertPosEqual(t, parser, 15, 1)

	require.Nil(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Fields: []mydump.Field{
			newStringField("\n", false),
			nullDatum,
			newStringField(`!N`, false),
		},
		Length: 7,
	}, parser.LastRow())
	assertPosEqual(t, parser, 26, 2)

	cfg = config.CSVConfig{
		Separator: ",",
		Delimiter: `"`,
		EscapedBy: ``,
		NotNull:   false,
		Null:      []string{`NULL`},
	}

	parser, err = mydump.NewCSVParser(
		&cfg,
		NewStringReader(`"{""itemRangeType"":0,""itemContainType"":0,""shopRangeType"":1,""shopJson"":""[{\""id\"":\""A1234\"",\""shopName\"":\""AAAAAA\""}]""}"`),
		int64(config.ReadBlockSize), false)
	require.NoError(t, err)

	require.Nil(t, parser.readRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Fields: []mydump.Field{
			newStringField(`{"itemRangeType":0,"itemContainType":0,"shopRangeType":1,"shopJson":"[{\"id\":\"A1234\",\"shopName\":\"AAAAAA\"}]"}`, false),
		},
		Length: 115,
	}, parser.LastRow())
}
