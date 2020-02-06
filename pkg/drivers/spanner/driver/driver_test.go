package driver

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"cloud.google.com/go/spanner"

	"math/rand"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

const (
	EnvGoogleApplicationCredential = "GOOGLE_APPLICATION_CREDENTIALS"
	EnvGoogleProject               = "GOOGLE_CLOUD_PROJECT"
	EnvGoogleSpannerInstance       = "GOOGLE_CLOUD_SPANNER_INSTANCE"
)

type DBTest struct {
	test        *testing.T
	adminClient *database.DatabaseAdminClient
	client      *spanner.Client
	dsn         string
	db          *sql.DB
}

// Extracted from k8s.io/apimachinery@v0.17.0 -- /pkg/util/rand/rand.go
func randomName(n int) string {
	const (
		// We omit vowels from the set of available characters to reduce the chances
		// of "bad words" being formed.
		alphanums = "bcdfghjklmnpqrstvwxz2456789"
		// No. of bits required to index into alphanums string.
		alphanumsIdxBits = 5
		// Mask used to extract last alphanumsIdxBits of an int.
		alphanumsIdxMask = 1<<alphanumsIdxBits - 1
		// No. of random letters we can extract from a single int63.
		maxAlphanumsPerInt = 63 / alphanumsIdxBits
	)
	b := make([]byte, n)
	randomInt63 := rand.Int63()
	remaining := maxAlphanumsPerInt
	for i := 0; i < n; {
		if remaining == 0 {
			randomInt63, remaining = rand.Int63(), maxAlphanumsPerInt
		}
		if idx := int(randomInt63 & alphanumsIdxMask); idx < len(alphanums) {
			b[i] = alphanums[idx]
			i++
		}
		randomInt63 >>= alphanumsIdxBits
		remaining--
	}
	return string(b)
}

func (d *DBTest) ddl(statements ...string) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	d.ddlContext(ctx, statements...)
}

func (d *DBTest) dml(stmts ...spanner.Statement) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	d.dmlContext(ctx, stmts...)
}

func (d *DBTest) ddlContext(ctx context.Context, statements ...string) {
	req := &databasepb.UpdateDatabaseDdlRequest{
		Database:   d.dsn,
		Statements: statements,
	}
	op, err := d.adminClient.UpdateDatabaseDdl(ctx, req)
	if err != nil {
		d.test.Fatalf("could not run ddl statements: %v", err)
	}
	if err = op.Wait(ctx); err != nil {
		d.test.Fatalf("ddl statements failed: %v", err)
	}
}

func (d *DBTest) dmlContext(ctx context.Context, stmts ...spanner.Statement) {
	_, err := d.client.ReadWriteTransaction(ctx, func(ctx context.Context, rw *spanner.ReadWriteTransaction) error {
		_, err := rw.BatchUpdate(ctx, stmts)
		return err
	})
	if err != nil {
		d.test.Fatalf("dml statements failed: %v", err)
	}
}

func SkipIfNoCredentials(t *testing.T) {
	t.Helper()
	path := SkipOrGetenv(t, EnvGoogleApplicationCredential)
	info, err := os.Stat(path)
	if err != nil {
		t.Skipf("could not stat '%s': %v", path, err)
	}
	if !info.Mode().IsRegular() {
		t.Skipf("'%s' is not a regular file", path)
	}
}

func SkipOrGetenv(t *testing.T, env string) string {
	t.Helper()
	val := os.Getenv(env)
	if val == "" {
		t.Skipf("environment variable %s not set", env)
	}
	return val
}

func RunWithTempDatabase(t *testing.T, test func(t *testing.T, db *DBTest)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	RunWithTempDatabaseContext(ctx, t, test)
}

func RunWithTempDatabaseContext(ctx context.Context, t *testing.T, test func(t *testing.T, db *DBTest)) {
	t.Helper()
	SkipIfNoCredentials(t)
	project := SkipOrGetenv(t, EnvGoogleProject)
	instance := SkipOrGetenv(t, EnvGoogleSpannerInstance)

	adminClient, err := database.NewDatabaseAdminClient(context.Background())
	if err != nil {
		t.Fatalf("could not create admin client: %v", err)
	}
	dbname := fmt.Sprintf("testdb_%s", randomName(8))
	createDBReq := &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", project, instance),
		CreateStatement: fmt.Sprintf("CREATE DATABASE %s", dbname),
	}
	t.Logf("Creating Temporary Database: %s", dbname)
	createDBOp, err := adminClient.CreateDatabase(ctx, createDBReq)
	if err != nil {
		t.Fatalf("could not create database '%s': %v", dbname, err)
	}
	if _, err = createDBOp.Wait(ctx); err != nil {
		t.Fatalf("create database '%s' failed: %v", dbname, err)
	}
	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, dbname)
	client, err := spanner.NewClient(ctx, dsn)
	if err != nil {
		t.Fatalf("could not create database client '%s': %v", dbname, err)
	}
	defer func() {
		dropDBReq := &databasepb.DropDatabaseRequest{
			Database: dsn,
		}
		t.Logf("Dropping Temporary Database: %s", dbname)
		if err := adminClient.DropDatabase(context.Background(), dropDBReq); err != nil {
			t.Logf("failed to drop database '%s': %v", dbname, err)
		}
	}()
	db, err := sql.Open(Name, dsn)
	if err != nil {
		t.Fatalf("sql.Open(%s,%s) failed: %v", dbname, dsn, err)
	}
	defer db.Close()
	test(t, &DBTest{
		test:        t,
		adminClient: adminClient,
		client:      client,
		dsn:         dsn,
		db:          db,
	})
}

func TestDriverTransactionCommitIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	RunWithTempDatabaseContext(ctx, t, func(t *testing.T, dbt *DBTest) {
		dbt.ddl(`CREATE TABLE foo(f1 INT64) PRIMARY KEY (f1)`)
		dbt.dml(spanner.NewStatement("INSERT INTO foo (f1) VALUES (0)"))
		db := dbt.db
		countRows := func() int {
			r, err := db.Query("SELECT f1 FROM foo")
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			var rc int
			for r.Next() {
				rc++
			}
			return rc
		}
		txn, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := txn.Exec("INSERT INTO foo (f1) VALUES (1)"); err != nil {
			t.Fatalf("insert failed: %v", err)
		}
		// Before Commit
		if rc := countRows(); rc != 1 {
			t.Fatalf("before commit: expected row count == 1, got %d", rc)
		}
		if err := txn.Commit(); err != nil {
			t.Fatalf("commit error: %v", err)
		}
		// After Commit
		if rc := countRows(); rc != 2 {
			t.Fatalf("after commit: expected row count == 2, got %d", rc)
		}
	})
}

func TestDriverTransactionRollback(t *testing.T) {
	RunWithTempDatabase(t, func(t *testing.T, dbt *DBTest) {
		dbt.ddl(`CREATE TABLE foo(f1 INT64) PRIMARY KEY (f1)`)
		dbt.dml(spanner.NewStatement("INSERT INTO foo (f1) VALUES (0)"))
		db := dbt.db

		txn, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := txn.Exec("INSERT INTO foo (f1) VALUES (1)"); err != nil {
			t.Fatalf("insert failed: %v", err)
		}
		err = txn.Rollback()
		if err != nil {
			t.Fatalf("rollback failed: %v", err)
		}
		r, err := db.Query("SELECT f1 FROM foo")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		var rc int
		for r.Next() {
			rc++
		}
		if rc != 1 {
			t.Fatalf("expected rows-affected == 1, got %d", rc)
		}
	})
}

func TestDriverCommitInFailedTransaction(t *testing.T) {
	RunWithTempDatabase(t, func(t *testing.T, dbt *DBTest) {
		dbt.ddl(`CREATE TABLE foo(f1 INT64) PRIMARY KEY (f1)`)
		dbt.dml(spanner.NewStatement("INSERT INTO foo (f1) VALUES (0)"))
		db := dbt.db

		txn, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		_, err = txn.Exec("INSERT INTO foo (f1) VALUES (0)")
		if err == nil {
			t.Fatal("expected failure")
		}
		t.Logf("err: %v", err)
		err = txn.Commit()
		if err != ErrTxnFailed {
			t.Fatalf("expected ErrTxnFailed; got %#v", err)
		}
	})
}

func TestDriverExec(t *testing.T) {
	RunWithTempDatabase(t, func(t *testing.T, dbt *DBTest) {
		dbt.ddl(`CREATE TABLE temp (id INT64) PRIMARY KEY (id)`)
		db := dbt.db

		r, err := db.Exec("INSERT INTO temp (id) VALUES (0)")
		if err != nil {
			t.Fatal(err)
		}

		if n, _ := r.RowsAffected(); n != 1 {
			t.Fatalf("expected 1 row affected, not %d", n)
		}

		r, err = db.Exec("INSERT INTO temp (id) VALUES ($1), ($2), ($3)", 1, 2, 3)
		if err != nil {
			t.Fatal(err)
		}

		if n, _ := r.RowsAffected(); n != 3 {
			t.Fatalf("expected 3 rows affected, not %d", n)
		}
	})
}

func TestDriverSimpleQuery(t *testing.T) {
	RunWithTempDatabase(t, func(t *testing.T, dbt *DBTest) {
		db := dbt.db
		r, err := db.Query("select 1")
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		if !r.Next() {
			t.Fatal("expected row")
		}
	})
}

func TestDriverStatement(t *testing.T) {
	RunWithTempDatabase(t, func(t *testing.T, dbt *DBTest) {
		dbt.ddl(`CREATE TABLE foo(f1 INT64) PRIMARY KEY (f1)`)
		dbt.dml(spanner.NewStatement("INSERT INTO foo (f1) VALUES (0)"))
		db := dbt.db

		st, err := db.Prepare("SELECT 1 from foo")
		if err != nil {
			t.Fatal(err)
		}

		st1, err := db.Prepare("SELECT 2 from foo")
		if err != nil {
			t.Fatal(err)
		}

		r, err := st.Query()
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		if !r.Next() {
			t.Fatal("expected row")
		}

		var i int
		err = r.Scan(&i)
		if err != nil {
			t.Fatal(err)
		}

		if i != 1 {
			t.Fatalf("expected 1, got %d", i)
		}

		// st1

		r1, err := st1.Query()
		if err != nil {
			t.Fatal(err)
		}
		defer r1.Close()

		if !r1.Next() {
			if r.Err() != nil {
				t.Fatal(r1.Err())
			}
			t.Fatal("expected row")
		}

		err = r1.Scan(&i)
		if err != nil {
			t.Fatal(err)
		}

		if i != 2 {
			t.Fatalf("expected 2, got %d", i)
		}
	})
}

func TestDriverRowsCloseBeforeDone(t *testing.T) {
	RunWithTempDatabase(t, func(t *testing.T, dbt *DBTest) {
		dbt.ddl(`CREATE TABLE foo(f1 INT64) PRIMARY KEY (f1)`)
		dbt.dml(spanner.NewStatement("INSERT INTO foo (f1) VALUES (0)"))
		db := dbt.db

		r, err := db.Query("SELECT 1 from foo")
		if err != nil {
			t.Fatal(err)
		}

		err = r.Close()
		if err != nil {
			t.Fatal(err)
		}

		if r.Next() {
			t.Fatal("unexpected row")
		}

		if r.Err() != nil {
			t.Fatal(r.Err())
		}
	})
}

func TestDriverParameterCountMismatch(t *testing.T) {
	RunWithTempDatabase(t, func(t *testing.T, dbt *DBTest) {
		dbt.ddl(`CREATE TABLE foo(f1 INT64) PRIMARY KEY (f1)`)
		dbt.dml(spanner.NewStatement("INSERT INTO foo (f1) VALUES (0)"))
		db := dbt.db

		var notused int
		err := db.QueryRow("SELECT false FROM foo", 1).Scan(&notused)
		if err == nil {
			t.Fatal("expected err")
		}
		// make sure we clean up correctly
		err = db.QueryRow("SELECT 1 FROM foo").Scan(&notused)
		if err != nil {
			t.Fatal(err)
		}

		err = db.QueryRow("SELECT $1 FROM foo").Scan(&notused)
		if err == nil {
			t.Fatal("expected err")
		}
		// make sure we clean up correctly
		err = db.QueryRow("SELECT 1 FROM foo").Scan(&notused)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestDriverEncodeDecode(t *testing.T) {
	ddls := []string{`
CREATE TABLE encode_decode (
	id INT64 NOT NULL,
	b BOOL,
	bs BYTES(MAX),
	f64 FLOAT64,
	int0 INT64,	
	int1 INT64,
	int2 INT64,
	str STRING(MAX),
	ts TIMESTAMP,
) PRIMARY KEY (id)`,
	}
	RunWithTempDatabase(t, func(t *testing.T, dbt *DBTest) {
		db := dbt.db
		dbt.ddl(ddls...)
		bs := []byte{0, 1, 2}
		var nilint *int64
		ts := time.Date(2000, 1, 1, 2, 3, 4, 5, time.UTC)
		dml := spanner.NewStatement("INSERT INTO encode_decode (id,int0,str,f64,b,ts,bs,int1,int2) VALUES (@id,@p1,@p2,@p3,@p4,@p5,@p6,@p7,@p8)")
		dml.Params["id"] = 1
		dml.Params["p1"] = nilint
		dml.Params["p2"] = "foobar"
		dml.Params["p3"] = 3.14
		dml.Params["p4"] = false
		dml.Params["p5"] = ts
		dml.Params["p6"] = bs
		dml.Params["p7"] = int64(123)
		dml.Params["p8"] = int64(-321)
		dbt.dml(dml)

		stmt, err := db.Prepare("SELECT bs,str,int0,ts,b,int1,int2,f64 FROM encode_decode WHERE bs = $1 AND str = $2 AND int0 IS NULL")
		if err != nil {
			t.Fatalf("error preparing statement: %v", err)
		}
		exp1 := []byte{0, 1, 2}
		exp2 := "foobar"

		r, err := stmt.Query(exp1, exp2)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		if !r.Next() {
			if r.Err() != nil {
				t.Fatal(r.Err())
			}
			t.Fatal("expected row")
		}

		var got1 []byte
		var got2 string
		var got3 = sql.NullInt64{Valid: true}
		var got4 time.Time
		var got5, got6, got7, got8 interface{}
		err = r.Scan(&got1, &got2, &got3, &got4, &got5, &got6, &got7, &got8)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(exp1, got1) {
			t.Errorf("expected %q byte: %q", exp1, got1)
		}

		if !reflect.DeepEqual(exp2, got2) {
			t.Errorf("expected %q byte: %q", exp2, got2)
		}

		if got3.Valid {
			t.Fatal("expected invalid")
		}

		if got4.Year() != 2000 {
			t.Fatal("wrong year")
		}

		if got5 != false {
			t.Fatalf("expected false, got %q", got5)
		}

		if got6 != int64(123) {
			t.Fatalf("expected 123, got %d", got6)
		}

		if got7 != int64(-321) {
			t.Fatalf("expected -321, got %d", got7)
		}

		if got8 != float64(3.14) {
			t.Fatalf("expected 3.14, got %f", got8)
		}
	})
}

func TestDriverNoData(t *testing.T) {
	RunWithTempDatabase(t, func(t *testing.T, dbt *DBTest) {
		dbt.ddl(`CREATE TABLE foo(f1 INT64) PRIMARY KEY (f1)`)
		db := dbt.db
		st, err := db.Prepare("SELECT 1 FROM foo WHERE true = false")
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()

		r, err := st.Query()
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		if r.Next() {
			if r.Err() != nil {
				t.Fatal(r.Err())
			}
			t.Fatal("unexpected row")
		}

		_, err = db.Query("SELECT * FROM nonexistenttable WHERE age=$1", 20)
		if err == nil {
			t.Fatal("Should have raised an error on non existent table")
		}

		_, err = db.Query("SELECT * FROM nonexistenttable")
		if err == nil {
			t.Fatal("Should have raised an error on non existent table")
		}
	})
}

func TestDriverErrorOnExec(t *testing.T) {
	RunWithTempDatabase(t, func(t *testing.T, dbt *DBTest) {
		dbt.ddl(`CREATE TABLE foo(f1 INT64) PRIMARY KEY (f1)`)
		db := dbt.db

		txn, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Rollback()

		_, err = txn.Exec("INSERT INTO foo (f1) VALUES (0), (0)")
		if err == nil {
			t.Fatal("Should have raised error")
		}

		e, ok := err.(*spanner.Error)
		if !ok {
			t.Fatalf("expected *spanner.Error, got %#v", err)
		} else {
			code := spanner.ErrCode(e)
			if code != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument, got %s (%+v)", code.String(), e)
			}
		}
	})
}

func TestDriverErrorOnQuery(t *testing.T) {
	RunWithTempDatabase(t, func(t *testing.T, dbt *DBTest) {
		dbt.ddl(`CREATE TABLE foo(f1 INT64) PRIMARY KEY (f1)`)
		db := dbt.db

		txn, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Rollback()

		_, err = txn.Query("INSERT INTO foo (f1) VALUES (0), (0)")
		if err == nil {
			t.Fatal("Should have raised error")
		}

		e, ok := err.(*spanner.Error)
		if !ok {
			t.Fatalf("expected *spanner.Error, got %#v", err)
		} else {
			code := spanner.ErrCode(e)
			if code != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument, got %s (%+v)", code.String(), e)
			}
		}
	})
}

func TestDriverErrorConstraintViolation(t *testing.T) {
	RunWithTempDatabase(t, func(t *testing.T, dbt *DBTest) {
		dbt.ddl(`CREATE TABLE foo(f1 INT64, f2 INT64) PRIMARY KEY (f1)`, `CREATE UNIQUE INDEX ukey ON foo (f2)`)
		dbt.dml(spanner.NewStatement(`INSERT INTO foo (f1,f2) VALUES (0,0), (1,1)`))
		db := dbt.db

		txn, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Rollback()

		_, err = txn.Query("INSERT INTO foo (f1,f2) VALUES (2,0)")
		if err == nil {
			t.Fatal("Should have raised error")
		}

		e, ok := err.(*spanner.Error)
		if !ok {
			t.Fatalf("expected *spanner.Error, got %#v", err)
		} else {
			code := spanner.ErrCode(e)
			if code != codes.AlreadyExists {
				t.Fatalf("expected AlreadyExists, got %s (%+v)", code.String(), e)
			}
			t.Logf("error code: %+v", err)
		}
	})
}

func TestDriverErrorOnQueryRowSimpleQuery(t *testing.T) {
	RunWithTempDatabase(t, func(t *testing.T, dbt *DBTest) {
		dbt.ddl(`CREATE TABLE foo(f1 INT64) PRIMARY KEY (f1)`)
		db := dbt.db

		txn, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Rollback()

		var v int
		err = txn.QueryRow("INSERT INTO foo (f1) VALUES (0), (0)").Scan(&v)
		if err == nil {
			t.Fatal("Should have raised error")
		}

		e, ok := err.(*spanner.Error)
		if !ok {
			t.Fatalf("expected *spanner.Error, got %#v", err)
		} else {
			code := spanner.ErrCode(e)
			if code != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument, got %s (%+v)", code.String(), e)
			}
		}
	})
}

func _TestDriver(t *testing.T) {
	ddls := []string{
		`CREATE TABLE test_table (
	id INT64 NOT NULL,
	favorite_food STRING(MAX),
	favorite_number FLOAT64,
	garbage BYTES(MAX),
	likes_to_party BOOL,
	name STRING(MAX) NOT NULL,
	updated_timestamp TIMESTAMP NOT NULL,
) PRIMARY KEY (id)`,
	}
	RunWithTempDatabase(t, func(t *testing.T, dbt *DBTest) {
		dbt.ddl(ddls...)
		db := dbt.db
		stmt, err := db.Prepare(`INSERT INTO test_table (id,name,favorite_food,favorite_number,garbage,likes_to_party,updated_timestamp) VALUES (?,?,?,?,?,?,?)`)
		if err != nil {
			t.Fatalf("error preparing statement: %v", err)
		}
		for i := 0; i < 100; i++ {
			id := int64(i)
			user := fmt.Sprintf("user-%d", i+1)
			food := fmt.Sprintf("food-%d", i)
			num := float64(i)
			garbage := []byte("garbage")
			ltp := i%2 == 0
			t.Logf("INSERT[%d]: %s,%s,%f,%s,%t", id, user, food, num, garbage, ltp)
			res, err := stmt.Exec(id, user, food, num, garbage, ltp, time.Now())
			if err != nil {
				t.Fatalf("error creating table: %v", err)
			}
			ra, err := res.RowsAffected()
			if err != nil {
				t.Fatalf("error getting affected rows: %v", err)
			}
			if ra == 0 {
				t.Fatalf("no rows affected")
			}
		}
		txn, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("error creating transaction")
		}
		rows, err := txn.QueryContext(context.Background(), "SELECT id FROM test_table")
		if err != nil {
			if err := txn.Rollback(); err != nil {
				t.Errorf("rollback error: %v", err)
			}
			t.Fatalf("error querying: %v", err)
		}
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				if err := txn.Rollback(); err != nil {
					t.Errorf("rollback error: %v", err)
				}
				t.Fatalf("error scanning result: %v", err)
			}
			_, err := txn.ExecContext(context.Background(), "UPDATE test_table SET favorite_food = ?, updated_timestamp = ?, garbage = ? WHERE id = ?", "pizza", time.Now(), []byte("data"), id)
			if err != nil {
				if err := txn.Rollback(); err != nil {
					t.Errorf("rollback error: %v", err)
				}
				t.Fatalf("error updating: %v", err)
			}
		}
		if err := txn.Commit(); err != nil {
			t.Errorf("commit error: %v", err)
		}
	})
}
