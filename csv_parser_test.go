package mydump_test

import (
	"context"
	mydump "csvReader"
	"csvReader/config"
	"csvReader/types"
	"csvReader/worker"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

var ioWorkersForCSV = worker.NewPool(context.Background(), 5, "test_csv")

func assertPosEqual(t *testing.T, parser mydump.Parser, expectPos, expectRowID int64) {
	pos, rowID := parser.Pos()
	require.Equal(t, expectPos, pos)
	require.Equal(t, expectRowID, rowID)
}

var nullDatum types.Datum

func tpchDatums() [][]types.Datum {
	datums := make([][]types.Datum, 0, 3)
	datums = append(datums, []types.Datum{
		types.NewStringDatum("1"),
		types.NewStringDatum("goldenrod lavender spring chocolate lace"),
		types.NewStringDatum("Manufacturer#1"),
		types.NewStringDatum("Brand#13"),
		types.NewStringDatum("PROMO BURNISHED COPPER"),
		types.NewStringDatum("7"),
		types.NewStringDatum("JUMBO PKG"),
		types.NewStringDatum("901.00"),
		types.NewStringDatum("ly. slyly ironi"),
	})
	datums = append(datums, []types.Datum{
		types.NewStringDatum("2"),
		types.NewStringDatum("blush thistle blue yellow saddle"),
		types.NewStringDatum("Manufacturer#1"),
		types.NewStringDatum("Brand#13"),
		types.NewStringDatum("LARGE BRUSHED BRASS"),
		types.NewStringDatum("1"),
		types.NewStringDatum("LG CASE"),
		types.NewStringDatum("902.00"),
		types.NewStringDatum("lar accounts amo"),
	})
	datums = append(datums, []types.Datum{
		types.NewStringDatum("3"),
		types.NewStringDatum("spring green yellow purple cornsilk"),
		types.NewStringDatum("Manufacturer#4"),
		types.NewStringDatum("Brand#42"),
		types.NewStringDatum("STANDARD POLISHED BRASS"),
		types.NewStringDatum("21"),
		types.NewStringDatum("WRAP CASE"),
		types.NewStringDatum("903.00"),
		types.NewStringDatum("egular deposits hag"),
	})

	return datums
}

func datumsToString(datums [][]types.Datum, delimitor string, quote string, lastSep bool) string {
	var b strings.Builder
	doubleQuote := quote + quote
	for _, ds := range datums {
		for i, d := range ds {
			text := d.GetString()
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
	reader := mydump.NewStringReader(input)

	cfg := config.CSVConfig{
		Separator:   "|",
		Delimiter:   "",
		TrimLastSep: true,
	}

	parser, err := mydump.NewCSVParser(context.Background(), &cfg, reader, int64(config.ReadBlockSize), ioWorkersForCSV, false)
	require.NoError(t, err)
	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID:  1,
		Row:    datums[0],
		Length: 116,
	}, parser.LastRow())
	assertPosEqual(t, parser, 126, 1)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID:  2,
		Row:    datums[1],
		Length: 104,
	}, parser.LastRow())
	assertPosEqual(t, parser, 241, 2)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID:  3,
		Row:    datums[2],
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

		reader := mydump.NewStringReader(inputStr)
		parser, err := mydump.NewCSVParser(context.Background(), &cfg, reader, int64(config.ReadBlockSize), ioWorkersForCSV, false)
		require.NoError(t, err)

		for i, expectedParserPos := range allExpectedParserPos {
			require.Nil(t, parser.ReadRow())
			require.Equal(t, int64(i+1), parser.LastRow().RowID)
			require.Equal(t, datums[i], parser.LastRow().Row)
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

	parser, err := mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader("aaa,bbb,ccc\nzzz,yyy,xxx\n"), int64(config.ReadBlockSize), ioWorkersForCSV, false)
	require.NoError(t, err)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("aaa"),
			types.NewStringDatum("bbb"),
			types.NewStringDatum("ccc"),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 12, 1)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("zzz"),
			types.NewStringDatum("yyy"),
			types.NewStringDatum("xxx"),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 24, 2)

	// example 2, no trailing new lines

	parser, err = mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader("aaa,bbb,ccc\nzzz,yyy,xxx"), int64(config.ReadBlockSize), ioWorkersForCSV, false)
	require.NoError(t, err)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("aaa"),
			types.NewStringDatum("bbb"),
			types.NewStringDatum("ccc"),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 12, 1)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("zzz"),
			types.NewStringDatum("yyy"),
			types.NewStringDatum("xxx"),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 23, 2)

	// example 5, quoted fields

	parser, err = mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader(`"aaa","bbb","ccc"`+"\nzzz,yyy,xxx"), int64(config.ReadBlockSize), ioWorkersForCSV, false)
	require.NoError(t, err)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("aaa"),
			types.NewStringDatum("bbb"),
			types.NewStringDatum("ccc"),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 18, 1)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("zzz"),
			types.NewStringDatum("yyy"),
			types.NewStringDatum("xxx"),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 29, 2)

	// example 6, line breaks within fields

	parser, err = mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader(`"aaa","b
bb","ccc"
zzz,yyy,xxx`), int64(config.ReadBlockSize), ioWorkersForCSV, false)
	require.NoError(t, err)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("aaa"),
			types.NewStringDatum("b\nbb"),
			types.NewStringDatum("ccc"),
		},
		Length: 10,
	}, parser.LastRow())
	assertPosEqual(t, parser, 19, 1)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("zzz"),
			types.NewStringDatum("yyy"),
			types.NewStringDatum("xxx"),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 30, 2)

	// example 7, quote escaping

	parser, err = mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader(`"aaa","b""bb","ccc"`), int64(config.ReadBlockSize), ioWorkersForCSV, false)
	require.NoError(t, err)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("aaa"),
			types.NewStringDatum("b\"bb"),
			types.NewStringDatum("ccc"),
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

	parser, err := mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader(`"\"","\\","\?"
"\
",\N,\\N`), int64(config.ReadBlockSize), ioWorkersForCSV, false)
	require.NoError(t, err)

	require.NoError(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum(`"`),
			types.NewStringDatum(`\`),
			types.NewStringDatum("?"),
		},
		Length: 6,
	}, parser.LastRow())
	assertPosEqual(t, parser, 15, 1)

	require.NoError(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("\n"),
			nullDatum,
			types.NewStringDatum(`\N`),
		},
		Length: 7,
	}, parser.LastRow())
	assertPosEqual(t, parser, 26, 2)

	parser, err = mydump.NewCSVParser(
		context.Background(), &cfg,
		mydump.NewStringReader(`"\0\b\n\r\t\Z\\\  \c\'\""`),
		int64(config.ReadBlockSize), ioWorkersForCSV, false)
	require.NoError(t, err)

	require.NoError(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum(string([]byte{0, '\b', '\n', '\r', '\t', 26, '\\', ' ', ' ', 'c', '\'', '"'})),
		},
		Length: 23,
	}, parser.LastRow())

	cfg.UnescapedQuote = true
	parser, err = mydump.NewCSVParser(
		context.Background(), &cfg,
		mydump.NewStringReader(`3,"a string containing a " quote",102.20
`),
		int64(config.ReadBlockSize), ioWorkersForCSV, false)
	require.NoError(t, err)

	require.NoError(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("3"),
			types.NewStringDatum(`a string containing a " quote`),
			types.NewStringDatum("102.20"),
		},
		Length: 36,
	}, parser.LastRow())

	parser, err = mydump.NewCSVParser(
		context.Background(), &cfg,
		mydump.NewStringReader(`3,"a string containing a " quote","102.20"`),
		int64(config.ReadBlockSize), ioWorkersForCSV, false)
	require.NoError(t, err)

	require.NoError(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("3"),
			types.NewStringDatum(`a string containing a " quote`),
			types.NewStringDatum("102.20"),
		},
		Length: 36,
	}, parser.LastRow())

	parser, err = mydump.NewCSVParser(
		context.Background(), &cfg,
		mydump.NewStringReader(`"a"b",c"d"e`),
		int64(config.ReadBlockSize), ioWorkersForCSV, false)
	require.NoError(t, err)

	require.NoError(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum(`a"b`),
			types.NewStringDatum(`c"d"e`),
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

	parser, err := mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader(`"!"","!!","!\"
"!
",!N,!!N`), int64(config.ReadBlockSize), ioWorkersForCSV, false)
	require.NoError(t, err)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum(`"`),
			types.NewStringDatum(`!`),
			types.NewStringDatum(`\`),
		},
		Length: 6,
	}, parser.LastRow())
	assertPosEqual(t, parser, 15, 1)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("\n"),
			nullDatum,
			types.NewStringDatum(`!N`),
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
		context.Background(), &cfg,
		mydump.NewStringReader(`"{""itemRangeType"":0,""itemContainType"":0,""shopRangeType"":1,""shopJson"":""[{\""id\"":\""A1234\"",\""shopName\"":\""AAAAAA\""}]""}"`),
		int64(config.ReadBlockSize), ioWorkersForCSV, false)
	require.NoError(t, err)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum(`{"itemRangeType":0,"itemContainType":0,"shopRangeType":1,"shopJson":"[{\"id\":\"A1234\",\"shopName\":\"AAAAAA\"}]"}`),
		},
		Length: 115,
	}, parser.LastRow())
}
