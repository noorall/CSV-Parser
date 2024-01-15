package config

import (
	"errors"
	"fmt"
)

const ReadBlockSize int64 = 64 * 1024

type StringOrStringSlice []string

// UnmarshalTOML implements the toml.Unmarshaler interface.
func (s *StringOrStringSlice) UnmarshalTOML(in interface{}) error {
	switch v := in.(type) {
	case string:
		*s = []string{v}
	case []interface{}:
		*s = make([]string, 0, len(v))
		for _, vv := range v {
			vs, ok := vv.(string)
			if !ok {
				return errors.New(fmt.Sprintf("invalid string slice '%v'", in))
			}
			*s = append(*s, vs)
		}
	default:
		return errors.New(fmt.Sprintf("invalid string slice '%v'", in))
	}
	return nil
}

type CSVConfig struct {
	// Separator, Delimiter and Terminator should all be in utf8mb4 encoding.
	Separator         string              `toml:"separator" json:"separator"`
	Delimiter         string              `toml:"delimiter" json:"delimiter"`
	Terminator        string              `toml:"terminator" json:"terminator"`
	Null              StringOrStringSlice `toml:"null" json:"null"`
	Header            bool                `toml:"header" json:"header"`
	HeaderSchemaMatch bool                `toml:"header-schema-match" json:"header-schema-match"`
	TrimLastSep       bool                `toml:"trim-last-separator" json:"trim-last-separator"`
	NotNull           bool                `toml:"not-null" json:"not-null"`
	// deprecated, use `escaped-by` instead.
	BackslashEscape bool `toml:"backslash-escape" json:"backslash-escape"`
	// EscapedBy has higher priority than BackslashEscape, currently it must be a single character if set.
	EscapedBy string `toml:"escaped-by" json:"escaped-by"`

	// hide these options for lightning configuration file, they can only be used by LOAD DATA
	// https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-field-line-handling
	StartingBy     string `toml:"-" json:"-"`
	AllowEmptyLine bool   `toml:"-" json:"-"`
	// For non-empty Delimiter (for example quotes), null elements inside quotes are not considered as null except for
	// `\N` (when escape-by is `\`). That is to say, `\N` is special for null because it always means null.
	QuotedNullIsText bool `toml:"-" json:"-"`
	// ref https://dev.mysql.com/doc/refman/8.0/en/load-data.html
	// > If the field begins with the ENCLOSED BY character, instances of that character are recognized as terminating a
	// > field value only if followed by the field or line TERMINATED BY sequence.
	// This means we will meet unescaped quote in a quoted field
	// > The "BIG" boss      -> The "BIG" boss
	// This means we will meet unescaped quote in a unquoted field
	UnescapedQuote bool `toml:"-" json:"-"`
}
