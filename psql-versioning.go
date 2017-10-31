package psql_versioning

import "database/sql"
import (
	"fmt"
	"github.com/gabriel-araujjo/versioned-database"
)

type DB = sql.DB

type strategy struct {}

func (v *strategy) Version(db *DB) (int, error) {

	var version int

	rows, err := db.Query("SELECT description FROM pg_shdescription JOIN pg_database ON objoid = pg_database.oid WHERE datname = current_database()")

	if err != nil {
		return -1, err
	}

	defer rows.Close()

	if rows.Next() {
		err := rows.Scan(&version)
		if err != nil {
			return -1, err
		}
		return version, nil
	} else {
		return 0, nil
	}
}

func (v *strategy) SetVersion(db *DB, version int) error {

	var (
		databaseName string
		err          error
	)
	err = db.QueryRow("SELECT current_database()").Scan(&databaseName)
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf("COMMENT ON DATABASE %s IS '%d'", databaseName, version))


	if err != nil {
		return err
	}
	return nil
}

func init() {
	version.Register("psql-versioning", new(strategy))
}