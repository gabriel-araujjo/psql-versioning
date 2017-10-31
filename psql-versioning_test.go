package psql_versioning

import (
	"database/sql"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"testing"
	"fmt"
	"errors"
)

const queryDb = "SELECT current_database()"
const queryVersion = "SELECT description FROM pg_shdescription JOIN pg_database ON objoid = pg_database.oid WHERE datname = current_database()"
const dbName = "mock_db"

var driver = new(strategy)

func TestDBVersionWithoutVersion(t *testing.T) {
	var (
		rows *sqlmock.Rows
		db *sql.DB
		mock sqlmock.Sqlmock
		err error
	)

	rows = sqlmock.NewRows([]string {"description"})
	db, mock, err = sqlmock.New()

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery(queryVersion).WillReturnRows(rows)

	version, err := driver.Version(db)
	if err != nil {
		t.Fatal(err)
	}

	if version != 0 {
		t.Fatalf("Version must be 0 on start, but %d was returned", version)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expections: %s", err)
	}
}

func TestDBVersionWithError(t *testing.T) {
	var (
		db *sql.DB
		mock sqlmock.Sqlmock
		err error
	)

	db, mock, err = sqlmock.New()

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery(queryVersion).WillReturnError(errors.New(""))

	_, err = driver.Version(db)

	if err == nil {
		t.Fatal(err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expections: %s", err)
	}
}

func TestDBVersionWithVersion(t *testing.T) {
	var (
		rows *sqlmock.Rows
		db *sql.DB
		mock sqlmock.Sqlmock
		err error
	)

	rows = sqlmock.NewRows([]string {"description"}).AddRow("1")
	db, mock, err = sqlmock.New()

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery(queryVersion).WillReturnRows(rows)

	version, err := driver.Version(db)
	if err != nil {
		t.Fatal(err)
	}

	if version != 1 {
		t.Fatalf("Version must be 1, but %d was returned", version)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expections: %s", err)
	}
}

func TestDBWithMalformattedVersion(t *testing.T) {
	var (
		rows *sqlmock.Rows
		db *sql.DB
		mock sqlmock.Sqlmock
		err error
	)

	rows = sqlmock.NewRows([]string {"description"}).AddRow("Malformatted String")
	db, mock, err = sqlmock.New()

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery(queryVersion).WillReturnRows(rows)

	version, err := driver.Version(db)
	if version != -1 {
		t.Fatalf("Version of malformatted string must be -1, %d was returned instead", version)
	}

	if err == nil {
		t.Fatal("An error must be returned when a malformatted string is returned by db")
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expections: %s", err)
	}
}

func TestDBSetVersion(t *testing.T) {
	var (
		dbNameRows *sqlmock.Rows
		db         *sql.DB
		mock       sqlmock.Sqlmock
		err        error
	)

	expectedVersion := 1
	dbNameRows = sqlmock.NewRows([]string {"current_database"}).AddRow(dbName)
	db, mock, err = sqlmock.New()

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery(queryDb).WillReturnRows(dbNameRows)
	mock.ExpectExec(fmt.Sprintf("COMMENT ON DATABASE %s IS '%d'", dbName, expectedVersion)).WillReturnResult(sqlmock.NewResult(0, 0))

	err = driver.SetVersion(db, expectedVersion)

	if err != nil {
		t.Fatalf("An error occurred while setting version: %e", err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expections: %s", err)
	}
}

func TestDBSetVersionCurrentDbQueryFails(t *testing.T) {
	var (
		db         *sql.DB
		mock       sqlmock.Sqlmock
		err        error
	)

	expectedVersion := 1
	db, mock, err = sqlmock.New()

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery(queryDb).WillReturnError(errors.New(""))

	err = driver.SetVersion(db, expectedVersion)

	if err == nil {
		t.Fatal("SetVersion did not pass out error when db name query fails")
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expections: %s", err)
	}
}

func TestDBSetVersionCommentOnDbFails(t *testing.T) {
	var (
		dbNameRows *sqlmock.Rows
		db         *sql.DB
		mock       sqlmock.Sqlmock
		err        error
	)

	expectedVersion := 1
	dbNameRows = sqlmock.NewRows([]string {"current_database"}).AddRow(dbName)
	db, mock, err = sqlmock.New()

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery(queryDb).WillReturnRows(dbNameRows)
	mock.ExpectExec(fmt.Sprintf("COMMENT ON DATABASE %s IS '%d'", dbName, expectedVersion)).WillReturnError(errors.New(""))

	err = driver.SetVersion(db, expectedVersion)

	if err == nil {
		t.Fatal("SetVersion did not pass out error when comment on db fails")
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("there were unfulfilled expections: %s", err)
	}
}