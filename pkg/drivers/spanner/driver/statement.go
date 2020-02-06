package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"

	"cloud.google.com/go/spanner"
)

type Statement struct {
	conn *conn
	sql  string
	args []string
}

func paramName(p int) string {
	return "param_" + strconv.FormatInt(int64(p), 10)
}

type statmentBuilder struct {
	pi   int
	sb   *strings.Builder
	args []string
}

func (s *statmentBuilder) WriteByte(b byte) error {
	return s.sb.WriteByte(b)
}

func (s *statmentBuilder) AutoParam() {
	s.pi++
	s.WriteParam(s.pi)
}

func (s *statmentBuilder) WriteParam(p int) {
	if p > len(s.args) {
		newArgs := make([]string, p)
		copy(newArgs, s.args)
		s.args = newArgs
	}
	pn := paramName(p)
	s.args[p-1] = pn
	s.sb.WriteString("@" + pn)
}

func (s *statmentBuilder) Statement() *Statement {
	return &Statement{
		sql:  s.sb.String(),
		args: s.args,
	}
}

func parseStatement(q string) *Statement {
	sb := &statmentBuilder{
		sb: &strings.Builder{},
	}
	r := strings.NewReader(q)
	var escaped bool
	b, err := r.ReadByte()
	for {
		if escaped {
			sb.WriteByte(b)
			escaped = false
		} else {
			switch b {
			case 0:
				break
			case '\\':
				escaped = true
			case '$':
				b, err = parseArg(r, sb)
				continue
			case '?':
				sb.AutoParam()
			default:
				sb.WriteByte(b)
			}
			if err != nil {
				break
			}
		}
		b, err = r.ReadByte()
	}
	return sb.Statement()
}

func parseArg(r *strings.Reader, stmt *statmentBuilder) (byte, error) {
	sb := &strings.Builder{}
	var b byte
	var err error
	for err == nil {
		b, err = r.ReadByte()
		if b >= '0' && b <= '9' {
			sb.WriteByte(b)
			continue
		}
		break
	}
	arg := sb.String()
	pn, err := strconv.ParseInt(arg, 10, 64)
	if err != nil {
		return b, err
	}
	stmt.WriteParam(int(pn))
	return b, err
}

func (s *Statement) spannerStatement(args []driver.NamedValue) (*spanner.Statement, error) {
	stmt := spanner.NewStatement(s.sql)
	if len(s.args) != len(args) {
		return nil, fmt.Errorf("spanner/statement: number of arguments in statement do not match")
	}
	for _, argv := range args {
		pn := s.args[argv.Ordinal-1]
		stmt.Params[pn] = argv.Value
	}
	return &stmt, nil
}

func (s *Statement) Close() error {
	return nil
}

func (s *Statement) NumInput() int {
	return len(s.args)
}

func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	nv := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		nv[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   arg,
		}
	}
	return s.ExecContext(context.Background(), nv)
}

func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	nv := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		nv[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   arg,
		}
	}
	return s.QueryContext(context.Background(), nv)
}

func (s *Statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	stmt, err := s.spannerStatement(args)
	if err != nil {
		return nil, err
	}
	return s.conn.exec(ctx, *stmt)
}

func (s *Statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	stmt, err := s.spannerStatement(args)
	if err != nil {
		return nil, err
	}
	return s.conn.query(ctx, *stmt)
}
