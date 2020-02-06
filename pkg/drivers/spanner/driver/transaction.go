package driver

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
)

func newTransaction(ctx context.Context, readonly bool) *transaction {
	return &transaction{
		ctx:        ctx,
		readonly:   readonly,
		txnOpCh:    make(chan transactionOp, 1),
		rollbackCh: make(chan struct{}, 1),
		resultCh:   make(chan error, 1),
	}
}

type transactionOp = func(ctx context.Context, ro *spanner.ReadOnlyTransaction, rw *spanner.ReadWriteTransaction) error

type transaction struct {
	ctx        context.Context
	readonly   bool
	txnOpCh    chan transactionOp
	rollbackCh chan struct{}
	resultCh   chan error
	err        error
}

func (t *transaction) running() bool {
	if t == nil {
		return false
	}
	return t.txnOpCh != nil
}

func (t *transaction) failed() bool {
	return t.err != nil
}

func (t *transaction) finish() {
	close(t.txnOpCh)
	t.err = <-t.resultCh
	t.txnOpCh = nil
	t.resultCh = nil
}

func (t *transaction) Commit() error {
	t.finish()
	if t.failed() {
		fmt.Println(t.err)
		return ErrTxnFailed
	}
	return nil
}

func (t *transaction) Rollback() error {
	close(t.rollbackCh)
	t.finish()
	if t.err != ErrRollback {
		return t.err
	}
	return nil
}

func (t *transaction) run(client *spanner.Client) {
	defer close(t.resultCh)
	switch t.readonly {
	case true:
		t.resultCh <- t.runTx(t.ctx, client.ReadOnlyTransaction(), nil)
		return
	case false:
		var once bool
		_, err := client.ReadWriteTransaction(t.ctx, func(ctx context.Context, rw *spanner.ReadWriteTransaction) error {
			if once { // cannot support retries in-client; retries must be done outside the client
				return ErrTxnRetry
			}
			once = true
			return t.runTx(ctx, nil, rw)
		})
		if err != nil {
			t.resultCh <- err
			return
		}
	}
}

func (t *transaction) runTx(ctx context.Context, ro *spanner.ReadOnlyTransaction, rw *spanner.ReadWriteTransaction) error {
	var result error
	for {
		select {
		case op, ok := <-t.txnOpCh:
			if !ok {
				return result
			}
			if result = op(ctx, ro, rw); result != nil {
				return result
			}
		case <-t.rollbackCh:
			return ErrRollback
		}
	}
}
