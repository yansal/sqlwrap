package sqlwrap

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/lib/pq"
)

func new() (*Connector, error) {
	pqconnector, err := pq.NewConnector("sslmode=disable")
	if err != nil {
		return nil, err
	}
	return NewConnector(pqconnector), nil
}

func TestConnectHook(t *testing.T) {
	connector, err := new()
	if err != nil {
		t.Fatal(err)
	}
	var ok bool
	connector.ConnectHook = func(ctx context.Context, err error, duration time.Duration) {
		t.Log(duration)
		ok = true
	}

	db := sql.OpenDB(connector)
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("expected ConnectHook to be called")
	}
}

func TestQueryHook(t *testing.T) {
	connector, err := new()
	if err != nil {
		t.Fatal(err)
	}
	var (
		ok    bool
		query = `select now()`
	)
	connector.QueryHook = func(ctx context.Context, query string, values []driver.NamedValue, err error, duration time.Duration) {
		t.Logf("%s (%v)", query, duration)
		ok = true
	}

	db := sql.OpenDB(connector)
	var now time.Time
	if err := db.QueryRowContext(context.Background(), query).Scan(&now); err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Error("expected QueryHook to be called")
	}
}
