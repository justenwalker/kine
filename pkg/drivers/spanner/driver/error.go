package driver

type Error string

func (e Error) Error() string {
	return string(e)
}

const (
	ErrRollback = Error("spanner: transaction rolled back")
	ErrTxnReadOnly = Error("spanner: transaction is read-only")
	ErrTxnStarted = Error("spanner: transaction already started")
	ErrTxnFailed = Error("spanner: transaction failed")
	ErrTxnRetry = Error("spanner: transaction needs to be retried")
	ErrUnsupported = Error("spanner: operation unsupported")
)
