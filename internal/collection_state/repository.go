package collection_state

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/turbot/go-kit/files"
	"github.com/turbot/tailpipe/internal/filepaths"
	"log"
	"log/slog"
	"os"
)

const (
	collectionStateTableName = "collection_state"
)

type Repository struct {
	// location of the collection state duck db files
	dbFile string
	db     *sql.DB
}

func NewRepository() (*Repository, error) {
	r := &Repository{dbFile: filepaths.CollectionStateDbFilePath()}

	// open the repository
	if err := r.open(); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Repository) open() error {
	if files.FileExists(r.dbFile) {
		if db, err := r.validateDb(); err == nil {
			// db exists and is valid
			r.db = db
			return nil
		}

		slog.Warn("Invalid collection state db file, deleting and recreating", "file", r.dbFile)
		// delete db file
		if err := os.Remove(r.dbFile); err != nil {
			return fmt.Errorf("could not delete invalid collection state db file %s: %w", r.dbFile, err)
		}
		// fall through to recreate
	}

	return r.initDb()
}

func (r *Repository) initDb() error {
	// Connect to DuckDB
	db, err := sql.Open("duckdb", r.dbFile)
	if err != nil {
		return fmt.Errorf("could not create collection state db file %s: %w", r.dbFile, err)
	}

	// Create table
	createTableSQL := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		partition_name VARCHAR PRIMARY KEY,
		collection_state VARCHAR
	);`, collectionStateTableName)

	_, err = db.Exec(createTableSQL)
	if err != nil {
		db.Close()
		return fmt.Errorf("could not create collection state db table: %w", err)
	}
	// sgtore db
	r.db = db

	return nil
}

// close
func (r *Repository) Close() error {
	if r.db == nil {
		return fmt.Errorf("repository is not open")
	}

	if r.db != nil {
		return r.db.Close()
	}
	return nil
}

func (r *Repository) validateDb() (db *sql.DB, err error) {
	defer func() {
		// ensure db is closed if there is an error
		if err != nil && db != nil {
			db.Close()
		}
	}()

	// open the db file and verify the schema
	// Open the DuckDB database file
	db, err = sql.Open("duckdb", r.dbFile)
	if err != nil {
		return nil, fmt.Errorf("could not open collection state db file %s: %w", r.dbFile, err)
	}

	// Check if the collection state table exists
	tableExists := false
	rows, err := db.Query(fmt.Sprintf(`SELECT name FROM sqlite_master WHERE type='table' AND name='%s';`, collectionStateTableName))
	if err != nil {
		return nil, fmt.Errorf("failed to check if table exists: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		tableExists = true
	}

	if !tableExists {
		return nil, fmt.Errorf("table '%s' does not exist", collectionStateTableName)
	}

	// Validate the schema of the collection state table
	rows, err = db.Query(fmt.Sprintf(`PRAGMA table_info('%s');`, collectionStateTableName))
	if err != nil {
		log.Fatalf("Failed to get table schema: %v", err)
	}
	defer rows.Close()

	var colName, colType string
	correctSchema := true
	expectedSchema := map[string]string{
		"partition_name":   "VARCHAR",
		"collection_state": "VARCHAR",
	}

	for rows.Next() {
		var cid int
		var notnull bool
		var pk bool
		var dfltValue sql.NullString
		if err := rows.Scan(&cid, &colName, &colType, &notnull, &dfltValue, &pk); err != nil {
			log.Fatalf("Failed to scan table schema: %v", err)
		}
		expectedType, exists := expectedSchema[colName]
		if !exists || colType != expectedType {
			correctSchema = false
			break
		}
	}

	if !correctSchema {
		return nil, fmt.Errorf("table '%s' does not have the correct schema", collectionStateTableName)
	}
	return db, nil
}

func (r *Repository) Load(partitionName string) (string, error) {
	if r.db == nil {
		return "", fmt.Errorf("repository is not open")
	}

	var collectionState string
	err := r.db.QueryRow(fmt.Sprintf("SELECT collection_state FROM %s WHERE partition_name = ?", collectionStateTableName), partitionName).Scan(&collectionState)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("could not load collection state data for partition %s: %w", partitionName, err)
	}

	return collectionState, nil
}

func (r *Repository) Save(partitionName, collectionState string) error {
	if r.db == nil {
		return fmt.Errorf("repository is not open")
	}

	_, err := r.db.Exec(fmt.Sprintf("INSERT OR REPLACE INTO %s (partition_name, collection_state) VALUES (?, ?)", collectionStateTableName), partitionName, collectionState)
	if err != nil {
		return fmt.Errorf("could not save collection state data for partition %s: %w", partitionName, err)
	}
	return nil
}
