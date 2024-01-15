package types

type Datum struct {
	val    string
	isNull bool
}

func (d *Datum) SetString(val string) {
	d.val = val
}

func (d *Datum) GetString() string {
	return d.val
}

func (d *Datum) SetNull() {
	d.isNull = true
}

func NewStringDatum(val string) Datum {
	return Datum{
		val:    val,
		isNull: false,
	}
}
