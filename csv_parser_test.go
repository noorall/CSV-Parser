package mydump_test

import (
	mydump "csvReader"
	"github.com/stretchr/testify/require"
	"io"
	"strings"
	"testing"
)

// TODO: rewrite test case

func NewStringReader(str string) io.Reader {
	return strings.NewReader(str)
}

func newStringField(val string, isNull bool) mydump.Field {
	return mydump.Field{
		Val:    val,
		IsNull: isNull,
	}
}
func assertPosEqual(t *testing.T, parser *mydump.CSVParser, pos int64) {
	require.Equal(t, parser.Pos(), pos)
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
	input := datumsToString(datums, "|", "", true)
	reader := strings.NewReader(input)

	cfg := mydump.CSVConfig{
		FieldTerminatedBy: "|",
		FieldEnclosedBy:   "",
		TrimLastSep:       true,
	}

	parser, err := mydump.NewCSVParser(&cfg, reader, int64(mydump.ReadBlockSize), false, false)
	require.NoError(t, err)

	var row []mydump.Field

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, datums[0], row)
	require.Equal(t, parser.Pos(), int64(126))
	assertPosEqual(t, parser, 126)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, datums[1], row)
	assertPosEqual(t, parser, 241)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, datums[2], row)
	assertPosEqual(t, parser, 369)

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

		cfg := mydump.CSVConfig{
			FieldTerminatedBy: SepAndQuote[0],
			FieldEnclosedBy:   SepAndQuote[1],
			TrimLastSep:       false,
		}

		reader := NewStringReader(inputStr)
		parser, err := mydump.NewCSVParser(&cfg, reader, int64(mydump.ReadBlockSize), false, false)
		require.NoError(t, err)

		for i, expectedParserPos := range allExpectedParserPos {
			row, err := parser.Read()
			require.Nil(t, err)
			require.Equal(t, datums[i], row)
			assertPosEqual(t, parser, int64(expectedParserPos))
		}

	}
}

func TestRFC4180(t *testing.T) {
	cfg := mydump.CSVConfig{
		FieldTerminatedBy: ",",
		FieldEnclosedBy:   `"`,
	}

	// example 1, trailing new lines

	parser, err := mydump.NewCSVParser(&cfg, NewStringReader("aaa,bbb,ccc\nzzz,yyy,xxx\n"), int64(mydump.ReadBlockSize), false, false)
	require.NoError(t, err)

	var row []mydump.Field

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []mydump.Field{
		newStringField("aaa", false),
		newStringField("bbb", false),
		newStringField("ccc", false),
	}, row)
	assertPosEqual(t, parser, 12)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []mydump.Field{
		newStringField("zzz", false),
		newStringField("yyy", false),
		newStringField("xxx", false),
	}, row)
	assertPosEqual(t, parser, 24)

	// example 2, no trailing new lines

	parser, err = mydump.NewCSVParser(&cfg, NewStringReader("aaa,bbb,ccc\nzzz,yyy,xxx"), int64(mydump.ReadBlockSize), false, false)
	require.NoError(t, err)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []mydump.Field{
		newStringField("aaa", false),
		newStringField("bbb", false),
		newStringField("ccc", false),
	}, row)
	assertPosEqual(t, parser, 12)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []mydump.Field{
		newStringField("zzz", false),
		newStringField("yyy", false),
		newStringField("xxx", false),
	}, row)
	assertPosEqual(t, parser, 23)

	// example 5, quoted fields

	parser, err = mydump.NewCSVParser(&cfg, NewStringReader(`"aaa","bbb","ccc"`+"\nzzz,yyy,xxx"), int64(mydump.ReadBlockSize), false, false)
	require.NoError(t, err)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []mydump.Field{
		newStringField("aaa", false),
		newStringField("bbb", false),
		newStringField("ccc", false),
	}, row)
	assertPosEqual(t, parser, 18)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []mydump.Field{
		newStringField("zzz", false),
		newStringField("yyy", false),
		newStringField("xxx", false),
	}, row)
	assertPosEqual(t, parser, 29)

	// example 6, line breaks within fields

	parser, err = mydump.NewCSVParser(&cfg, NewStringReader(`"aaa","b
bb","ccc"
zzz,yyy,xxx`), int64(mydump.ReadBlockSize), false, false)
	require.NoError(t, err)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []mydump.Field{
		newStringField("aaa", false),
		newStringField("b\nbb", false),
		newStringField("ccc", false),
	}, row)
	assertPosEqual(t, parser, 19)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []mydump.Field{
		newStringField("zzz", false),
		newStringField("yyy", false),
		newStringField("xxx", false),
	}, row)
	assertPosEqual(t, parser, 30)

	// example 7, quote escaping

	parser, err = mydump.NewCSVParser(&cfg, NewStringReader(`"aaa","b""bb","ccc"`), int64(mydump.ReadBlockSize), false, false)
	require.NoError(t, err)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []mydump.Field{
		newStringField("aaa", false),
		newStringField("b\"bb", false),
		newStringField("ccc", false),
	}, row)
	assertPosEqual(t, parser, 19)

}

func TestMySQL(t *testing.T) {
	cfg := mydump.CSVConfig{
		FieldTerminatedBy: ",",
		FieldEnclosedBy:   `"`,
		LineTerminatedBy:  "\n",
		FieldEscapedBy:    `\`,
		NotNull:           false,
		Null:              []string{`\N`},
	}

	parser, err := mydump.NewCSVParser(&cfg, NewStringReader(`"\"","\\","\?"
"\
",\N,\\N`), int64(mydump.ReadBlockSize), false, false)
	require.NoError(t, err)

	var row []mydump.Field

	row, err = parser.Read()
	require.NoError(t, err)
	require.Equal(t, []mydump.Field{
		newStringField(`"`, false),
		newStringField(`\`, false),
		newStringField("?", false),
	}, row)

	assertPosEqual(t, parser, 15)

	row, err = parser.Read()
	require.NoError(t, err)

	require.Equal(t, []mydump.Field{
		newStringField("\n", false),
		newStringField("\\N", true),
		newStringField(`\N`, false),
	}, row)

	assertPosEqual(t, parser, 26)

	parser, err = mydump.NewCSVParser(
		&cfg,
		NewStringReader(`"\0\b\n\r\t\Z\\\  \c\'\""`),
		int64(mydump.ReadBlockSize), false, false)
	require.NoError(t, err)

	row, err = parser.Read()
	require.NoError(t, err)
	require.Equal(t, []mydump.Field{
		newStringField(string([]byte{0, '\b', '\n', '\r', '\t', 26, '\\', ' ', ' ', 'c', '\'', '"'}), false),
	}, row)

	cfg.UnescapedQuote = true
	parser, err = mydump.NewCSVParser(
		&cfg,
		NewStringReader(`3,"a string containing a " quote",102.20
`),
		int64(mydump.ReadBlockSize), false, false)
	require.NoError(t, err)

	row, err = parser.Read()
	require.NoError(t, err)
	require.Equal(t, []mydump.Field{
		newStringField("3", false),
		newStringField(`a string containing a " quote`, false),
		newStringField("102.20", false),
	}, row)

	parser, err = mydump.NewCSVParser(
		&cfg,
		NewStringReader(`3,"a string containing a " quote","102.20"`),
		int64(mydump.ReadBlockSize), false, false)
	require.NoError(t, err)

	row, err = parser.Read()
	require.NoError(t, err)
	require.Equal(t, []mydump.Field{
		newStringField("3", false),
		newStringField(`a string containing a " quote`, false),
		newStringField("102.20", false),
	}, row)

	parser, err = mydump.NewCSVParser(
		&cfg,
		NewStringReader(`"a"b",c"d"e`),
		int64(mydump.ReadBlockSize), false, false)
	require.NoError(t, err)

	row, err = parser.Read()
	require.NoError(t, err)
	require.Equal(t, []mydump.Field{
		newStringField(`a"b`, false),
		newStringField(`c"d"e`, false),
	}, row)
}

func TestCustomEscapeChar(t *testing.T) {
	cfg := mydump.CSVConfig{
		FieldTerminatedBy: ",",
		FieldEnclosedBy:   `"`,
		FieldEscapedBy:    `!`,
		NotNull:           false,
		Null:              []string{`!N`},
	}

	parser, err := mydump.NewCSVParser(&cfg, NewStringReader(`"!"","!!","!\"
"!
",!N,!!N`), int64(mydump.ReadBlockSize), false, false)
	require.NoError(t, err)

	var row []mydump.Field

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []mydump.Field{
		newStringField(`"`, false),
		newStringField(`!`, false),
		newStringField(`\`, false),
	}, row)
	assertPosEqual(t, parser, 15)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []mydump.Field{
		newStringField("\n", false),
		newStringField(`!N`, true),
		newStringField(`!N`, false),
	}, row)
	assertPosEqual(t, parser, 26)

	cfg = mydump.CSVConfig{
		FieldTerminatedBy: ",",
		FieldEnclosedBy:   `"`,
		FieldEscapedBy:    ``,
		NotNull:           false,
		Null:              []string{`NULL`},
	}

	parser, err = mydump.NewCSVParser(
		&cfg,
		NewStringReader(`"{""itemRangeType"":0,""itemContainType"":0,""shopRangeType"":1,""shopJson"":""[{\""id\"":\""A1234\"",\""shopName\"":\""AAAAAA\""}]""}"`),
		int64(mydump.ReadBlockSize), false, false)
	require.NoError(t, err)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []mydump.Field{
		newStringField(`{"itemRangeType":0,"itemContainType":0,"shopRangeType":1,"shopJson":"[{\"id\":\"A1234\",\"shopName\":\"AAAAAA\"}]"}`, false),
	}, row)
}
