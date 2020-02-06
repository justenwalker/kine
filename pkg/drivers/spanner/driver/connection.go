package driver

import (
	"context"
	"database/sql/driver"
	"sync"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/spanner"
)

type conn struct {
	dsn    *DSN
	client *spanner.Client
	txn    *transaction
	lock   sync.RWMutex
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	stmt := parseStatement(query)
	stmt.conn = c
	return stmt, nil
}

func (c *conn) Close() error {
	c.client.Close()
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.txn != nil && c.txn.running() {
		return nil, ErrTxnStarted
	}
	c.txn = newTransaction(ctx, opts.ReadOnly)
	go c.txn.run(c.client)
	return c.txn, nil
}

func (c *conn) exec(ctx context.Context, stmt spanner.Statement) (driver.Result, error) {
	var result driver.Result
	var err error
	if c.txn.running() {
		if c.txn.failed() {
			return nil, ErrTxnFailed
		}
		doneCh := make(chan struct{})
		c.txn.txnOpCh <- func(ctx context.Context, ro *spanner.ReadOnlyTransaction, rw *spanner.ReadWriteTransaction) error {
			defer close(doneCh)
			if rw == nil {
				err = ErrTxnReadOnly
				return nil
			}
			result, err = c.update(ctx, stmt, rw)
			return err
		}
		<-doneCh
		return result, err
	}
	_, terr := c.client.ReadWriteTransaction(ctx, func(ctx context.Context, rw *spanner.ReadWriteTransaction) error {
		result, err = c.update(ctx, stmt, rw)
		return err
	})
	if terr != nil {
		return nil, terr
	}
	return result, err
}

type Result struct {
	Count int64
	Err   error
}

func (r Result) LastInsertId() (int64, error) {
	return 0, ErrUnsupported
}

func (r Result) RowsAffected() (int64, error) {
	if r.Err != nil {
		return 0, r.Err
	}
	return r.Count, nil
}

func (c *conn) update(ctx context.Context, stmt spanner.Statement, rw *spanner.ReadWriteTransaction) (driver.Result, error) {
	rc, err := rw.Update(ctx, stmt)
	if err != nil {
		return nil, err
	}
	return Result{
		Count: rc,
	}, nil
}

func (c *conn) query(ctx context.Context, stmt spanner.Statement) (driver.Rows, error) {
	var ri *spanner.RowIterator
	if c.txn.running() {
		if c.txn.failed() {
			return nil, ErrTxnFailed
		}
		riCh := make(chan *spanner.RowIterator, 1)
		c.txn.txnOpCh <- func(ctx context.Context, ro *spanner.ReadOnlyTransaction, rw *spanner.ReadWriteTransaction) error {
			defer close(riCh)
			if ro != nil {
				riCh <- ro.Query(ctx, stmt)
				return nil
			}
			riCh <- rw.Query(ctx, stmt)
			return nil
		}
		ri = <-riCh
	} else {
		ri = c.client.Single().Query(ctx, stmt)
	}
	row, err := ri.Next()
	if err != nil {
		if err == iterator.Done {
			return &Rows{}, nil
		}
		ri.Stop()
		return nil, err
	}
	return &Rows{
		cols: row.ColumnNames(),
		r1:   row,
		ri:   ri,
	}, nil
}

func (c *conn) txnRunning() bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.txn == nil {
		return false
	}
	return c.txn.running()
}

func NewConnection(dsn DSN) (driver.Conn, error) {
	c, err := spanner.NewClient(context.Background(), dsn.String())
	if err != nil {
		return nil, err
	}
	return &conn{
		dsn:    &dsn,
		client: c,
	}, nil
}
