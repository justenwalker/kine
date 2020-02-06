package spanner

import (
	"context"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"

	"github.com/rancher/kine/pkg/drivers/generic"
	"github.com/rancher/kine/pkg/drivers/spanner/driver"
	"github.com/rancher/kine/pkg/logstructured"
	"github.com/rancher/kine/pkg/logstructured/sqllog"
	"github.com/rancher/kine/pkg/server"
)

func New(ctx context.Context, dataSourceName string) (server.Backend, error) {
	dsn, _, err := driver.ParseConnectionURI(dataSourceName)
	if err != nil {
		return nil, err
	}
	dialect, err := generic.Open(ctx, driver.Name, dsn.String(), "$", true)
	if err != nil {
		return nil, err
	}
	dialect.NoAutoIncrement = true
	dialect.TranslateErr = func(err error) error {
		if code := spanner.ErrCode(err); code != codes.Unknown {
			switch code {
			case codes.AlreadyExists:
				return server.ErrKeyExists
			}
		}
		return err
	}
	dialect.Retry = func(err error) bool {
		if code := spanner.ErrCode(err); code != codes.Unknown {
			switch code {
			case codes.DeadlineExceeded:
				return true
			case codes.Canceled:
				return true
			case codes.Aborted:
				return true
			}
		}
		return false
	}
	setupDB(ctx, dsn.String())
	return logstructured.New(sqllog.New(dialect)), nil
}
