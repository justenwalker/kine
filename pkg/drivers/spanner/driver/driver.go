package driver

import (
	"database/sql"
	"database/sql/driver"
)

const Name = "google-spanner"
const Table = "kine"

var gDriver = SpannerDriver{}

func init() {
	sql.Register(Name, gDriver)
}

type SpannerDriver struct {
}

func (d SpannerDriver) Open(name string) (driver.Conn, error) {
	dsn, _, err := ParseConnectionURI(name)
	if err != nil {
		return nil, err
	}
	conn, err := NewConnection(*dsn)
	if err != nil {
		return nil, err
	}
	return conn, err
}
