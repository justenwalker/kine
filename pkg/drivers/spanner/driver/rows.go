package driver

import (
	"database/sql/driver"
	"io"
	"time"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/spanner"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
)

type Rows struct {
	cols []string
	r1   *spanner.Row
	ri   *spanner.RowIterator
}

func (r *Rows) Columns() []string {
	return r.cols
}

func (r *Rows) Close() error {
	if r.ri != nil {
		r.ri.Stop()
	}
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	if r.r1 == nil && r.ri == nil {
		return io.EOF
	}
	var row *spanner.Row
	var err error
	if r.r1 != nil {
		row = r.r1
		r.r1 = nil
	} else {
		row, err = r.ri.Next()
	}
	if err != nil {
		if err == iterator.Done {
			return io.EOF
		}
		return err
	}
	return decodeValues(dest, row)
}

type rowDecoder interface {
	Column(i int, ptr interface{}) error
}

func decodeValues(dest []driver.Value, dec rowDecoder) error {
	for i := range dest {
		var gcv spanner.GenericColumnValue
		if err := dec.Column(i, &gcv); err != nil {
			return err
		}
		val, err := convert(gcv)
		if err != nil {
			return err
		}
		dest[i] = val
	}
	return nil
}

func convert(g spanner.GenericColumnValue) (interface{}, error) {
	switch g.Type.Code {
	case sppb.TypeCode_BOOL:
		var val spanner.NullBool
		if err := g.Decode(&val); err != nil {
			return nil, err
		}
		if val.IsNull() {
			return nil, nil
		}
		return val.Bool, nil
	case sppb.TypeCode_INT64:
		var val spanner.NullInt64
		if err := g.Decode(&val); err != nil {
			return nil, err
		}
		if val.IsNull() {
			return nil, nil
		}
		return val.Int64, nil
	case sppb.TypeCode_FLOAT64:
		var val spanner.NullFloat64
		if err := g.Decode(&val); err != nil {
			return nil, err
		}
		if val.IsNull() {
			return nil, nil
		}
		return val.Float64, nil
	case sppb.TypeCode_TIMESTAMP:
		var val spanner.NullTime
		if err := g.Decode(&val); err != nil {
			return nil, err
		}
		if val.IsNull() {
			return nil, nil
		}
		return val.Time, nil
	case sppb.TypeCode_DATE:
		var val spanner.NullDate
		if err := g.Decode(&val); err != nil {
			return nil, err
		}
		if val.IsNull() {
			return nil, nil
		}
		return val.Date.In(time.UTC), nil
	case sppb.TypeCode_STRING:
		var val spanner.NullString
		if err := g.Decode(&val); err != nil {
			return nil, err
		}
		if val.IsNull() {
			return nil, nil
		}
		return val.StringVal, nil
	case sppb.TypeCode_BYTES:
		var val []byte
		err := g.Decode(&val)
		return val, err
	case sppb.TypeCode_STRUCT:
		var val []spanner.NullRow
		if err := g.Decode(&val); err != nil {
			return nil, err
		}
		if len(val) == 0 {
			return nil, nil
		}
		return &SubRows{
			cols: val[0].Row.ColumnNames(),
			rows: val,
		}, nil
	default:
		return nil, ErrUnsupported
	}
}

type SubRows struct {
	cols  []string
	index int
	rows  []spanner.NullRow
}

func (r *SubRows) Columns() []string {
	return r.cols
}

func (r *SubRows) Close() error {
	return nil
}

func (r *SubRows) Next(dest []driver.Value) error {
	for !r.rows[r.index].Valid {
		r.index++
	}
	row := r.rows[r.index].Row
	r.index++
	return decodeValues(dest, &row)
}
