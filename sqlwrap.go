package sqlwrap

import (
	"context"
	"database/sql/driver"
	"reflect"
	"time"
)

func NewConnector(wrapped driver.Connector) *Connector {
	return &Connector{wrapped: wrapped}
}

type Connector struct {
	wrapped driver.Connector

	ConnectHook func(context.Context, error, time.Duration)
	QueryHook   func(context.Context, string, []driver.NamedValue, error, time.Duration)
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	start := time.Now()
	conn, err := c.wrapped.Connect(ctx)
	if c.ConnectHook != nil {
		c.ConnectHook(ctx, err, time.Since(start))
	}
	return &Conn{
		wrapped:   conn,
		queryHook: c.QueryHook,
	}, err
}

func (c *Connector) Driver() driver.Driver {
	return c.wrapped.Driver()
}

type Conn struct {
	wrapped driver.Conn

	queryHook func(context.Context, string, []driver.NamedValue, error, time.Duration)
}

func (c *Conn) Begin() (driver.Tx, error) {
	tx, err := c.wrapped.Begin()
	return &Tx{wrapped: tx}, err
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.wrapped.(driver.ConnBeginTx).BeginTx(ctx, opts)
	return &Tx{wrapped: tx}, err
}

func (c *Conn) Close() error {
	return c.wrapped.Close()
}

func (c *Conn) Prepare(s string) (driver.Stmt, error) {
	stmt, err := c.wrapped.Prepare(s)
	return &Stmt{wrapped: stmt}, err
}

func (c *Conn) Ping(ctx context.Context) error {
	return c.wrapped.(driver.Pinger).Ping(ctx)
}

func (c *Conn) Exec(query string, values []driver.Value) (driver.Result, error) {
	result, err := c.wrapped.(driver.Execer).Exec(query, values)
	return &Result{wrapped: result}, err
}

func (c *Conn) ExecContext(ctx context.Context, query string, values []driver.NamedValue) (driver.Result, error) {
	result, err := c.wrapped.(driver.ExecerContext).ExecContext(ctx, query, values)
	return &Result{wrapped: result}, err
}

func (c *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	rows, err := c.wrapped.(driver.Queryer).Query(query, args)
	return &Rows{wrapped: rows}, err
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	start := time.Now()
	rows, err := c.wrapped.(driver.QueryerContext).QueryContext(ctx, query, args)
	if c.queryHook != nil {
		c.queryHook(ctx, query, args, err, time.Since(start))
	}

	return &Rows{wrapped: rows}, err
}

var (
	_ driver.ConnBeginTx    = &Conn{}
	_ driver.Pinger         = &Conn{}
	_ driver.Execer         = &Conn{}
	_ driver.ExecerContext  = &Conn{}
	_ driver.Queryer        = &Conn{}
	_ driver.QueryerContext = &Conn{}
)

type Tx struct{ wrapped driver.Tx }

func (tx *Tx) Commit() error {
	return tx.wrapped.Commit()
}

func (tx *Tx) Rollback() error {
	return tx.wrapped.Rollback()
}

type Stmt struct{ wrapped driver.Stmt }

func (s *Stmt) Close() error {
	return s.wrapped.Close()
}

func (s *Stmt) Exec(values []driver.Value) (driver.Result, error) {
	result, err := s.wrapped.Exec(values)
	return &Result{wrapped: result}, err
}

func (s *Stmt) NumInput() int {
	return s.wrapped.NumInput()
}

func (s *Stmt) Query(values []driver.Value) (driver.Rows, error) {
	rows, err := s.wrapped.Query(values)
	return &Rows{wrapped: rows}, err
}

type Result struct{ wrapped driver.Result }

func (r *Result) LastInsertId() (int64, error) {
	return r.wrapped.LastInsertId()
}

func (r *Result) RowsAffected() (int64, error) {
	return r.wrapped.RowsAffected()
}

type Rows struct{ wrapped driver.Rows }

func (r *Rows) Close() error {
	return r.wrapped.Close()
}

func (r *Rows) Columns() []string {
	return r.wrapped.Columns()
}

func (r *Rows) Next(values []driver.Value) error {
	return r.wrapped.Next(values)
}

func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	return r.wrapped.(driver.RowsColumnTypeDatabaseTypeName).ColumnTypeDatabaseTypeName(index)
}

func (r *Rows) ColumnTypeLength(index int) (int64, bool) {
	return r.wrapped.(driver.RowsColumnTypeLength).ColumnTypeLength(index)
}

func (r *Rows) ColumnTypePrecisionScale(index int) (int64, int64, bool) {
	return r.wrapped.(driver.RowsColumnTypePrecisionScale).ColumnTypePrecisionScale(index)
}

func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	return r.wrapped.(driver.RowsColumnTypeScanType).ColumnTypeScanType(index)
}

func (r *Rows) HasNextResultSet() bool {
	return r.wrapped.(driver.RowsNextResultSet).HasNextResultSet()
}

func (r *Rows) NextResultSet() error {
	return r.wrapped.(driver.RowsNextResultSet).NextResultSet()
}

var (
	_ driver.RowsColumnTypeDatabaseTypeName = &Rows{}
	_ driver.RowsColumnTypeLength           = &Rows{}
	_ driver.RowsColumnTypePrecisionScale   = &Rows{}
	_ driver.RowsColumnTypeScanType         = &Rows{}
	_ driver.RowsNextResultSet              = &Rows{}
)
