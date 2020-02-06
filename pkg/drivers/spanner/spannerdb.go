package spanner

import (
	"context"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/spanner"

	"github.com/rancher/kine/pkg/drivers/spanner/driver"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

const (
	spannerDataRevision = int64(0)
)

const (
	KineTable     = "kine"
)

var (
	schema = []struct {
		Name   string
		Schema []string
	}{
		{KineTable,
			[]string{`
CREATE TABLE ` + KineTable + ` (
	id INT64 NOT NULL,
	name STRING(630),
	created INT64,
	deleted INT64,
	create_revision INT64,
	prev_revision INT64,
	lease INT64,
	value BYTES(MAX),
	old_value BYTES(MAX),
) PRIMARY KEY (id)`,
				`CREATE INDEX kine_name_index ON kine (name)`,
				`CREATE UNIQUE INDEX kine_name_prev_revision_uindex ON kine (name,prev_revision)`,
			},
		},
	}
)

type spannerDB struct {
	dsn    *driver.DSN
	admin  *database.DatabaseAdminClient
	client *spanner.Client
}

func setupDB(ctx context.Context, dataSourceName string) error {
	dsn, _, err := driver.ParseConnectionURI(dataSourceName)
	if err != nil {
		return err
	}
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return  err
	}
	getReq := &databasepb.GetDatabaseRequest{
		Name: dsn.String(),
	}

	// Find DB
	if _, err := adminClient.GetDatabase(ctx, getReq); err != nil {
		return  err
	}

	client, err := spanner.NewClient(ctx, dsn.String())
	if err != nil {
		return  err
	}

	db := &spannerDB{
		dsn:    dsn,
		admin:  adminClient,
		client: client,
	}
	if err := db.setup(ctx); err != nil {
		return err
	}
	return  nil
}

func (db *spannerDB) tableExists(ctx context.Context, table string) (bool, error) {
	// Test if table exists
	stmt := spanner.NewStatement("SELECT t.table_name FROM information_schema.tables AS t WHERE t.table_catalog = '' AND t.table_schema = '' AND t.table_name = @tbl")
	stmt.Params["tbl"] = table
	ri := db.client.Single().Query(ctx, stmt)
	_, err := ri.Next()
	if err == nil { // exists
		return true, nil
	}
	if err != iterator.Done {
		return false, err
	}
	return false, nil
}

func (db *spannerDB) runDDLs(ctx context.Context, statements ...string) error {
	updateReq := &databasepb.UpdateDatabaseDdlRequest{
		Database:   db.dsn.String(),
		Statements: statements,
	}
	op, err := db.admin.UpdateDatabaseDdl(ctx, updateReq)
	if err != nil {
		return err
	}
	return op.Wait(ctx)
}

func (db *spannerDB) runDML(ctx context.Context, statements ...spanner.Statement) error {
	_, err := db.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := tx.BatchUpdate(ctx, statements)
		return err
	})
	return err
}

func (db *spannerDB) setup(ctx context.Context) error {
	for _, tbl := range schema {
		exists, err := db.tableExists(ctx, tbl.Name)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		if err = db.runDDLs(ctx, tbl.Schema...); err != nil {
			return err
		}
	}
	return nil
}
