package paging

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/turbot/go-kit/files"
	"log"
	"log/slog"
	"os"
	"path/filepath"
)

const (
	pagingDbName    = "paging.db"
	pagingTableName = "paging"
)

type Repository struct {
	// location of the paging duck db files
	dbFile string
	db     *sql.DB
}

func NewRepository(dataDir string) (*Repository, error) {
	// ensure the directory exists
	if !files.DirectoryExists(dataDir) {
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return nil, fmt.Errorf("could not create paging data directory %s: %w", dataDir, err)
		}
	}
	r := &Repository{dbFile: filepath.Join(dataDir, pagingDbName)}

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

		slog.Warn("Invalid paging db file, deleting and recreating", "file", r.dbFile)
		// delete db file
		if err := os.Remove(r.dbFile); err != nil {
			return fmt.Errorf("could not delete invalid paging db file %s: %w", r.dbFile, err)
		}
		// fall through to recreate
	}

	return r.initDb()
}

func (r *Repository) initDb() error {
	// Connect to DuckDB
	db, err := sql.Open("duckdb", r.dbFile)
	if err != nil {
		return fmt.Errorf("could not create paging db file %s: %w", r.dbFile, err)
	}

	// Create table
	createTableSQL := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		collection_name VARCHAR PRIMARY KEY,
		paging_data VARCHAR
	);`, pagingTableName)

	_, err = db.Exec(createTableSQL)
	if err != nil {
		db.Close()
		return fmt.Errorf("could not create paging db table: %w", err)
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
		return nil, fmt.Errorf("could not open paging db file %s: %w", r.dbFile, err)
	}

	// Check if the collections table exists
	tableExists := false
	rows, err := db.Query(fmt.Sprintf(`SELECT name FROM sqlite_master WHERE type='table' AND name='%s';`, pagingTableName))
	if err != nil {
		return nil, fmt.Errorf("failed to check if table exists: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		tableExists = true
	}

	if !tableExists {
		return nil, fmt.Errorf("table '%s' does not exist", pagingTableName)
	}

	// Validate the schema of the collections table
	rows, err = db.Query(fmt.Sprintf(`PRAGMA table_info('%s');`, pagingTableName))
	if err != nil {
		log.Fatalf("Failed to get table schema: %v", err)
	}
	defer rows.Close()

	var colName, colType string
	correctSchema := true
	expectedSchema := map[string]string{
		"collection_name": "VARCHAR",
		"paging_data":     "VARCHAR",
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
		return nil, fmt.Errorf("table '%s' does not have the correct schema", pagingTableName)
	}
	return db, nil
}

func (r *Repository) Load(collectionName string) (string, error) {
	if r.db == nil {
		return "", fmt.Errorf("repository is not open")
	}

	var pagingData string
	err := r.db.QueryRow(fmt.Sprintf("SELECT paging_data FROM %s WHERE collection_name = ?", pagingTableName), collectionName).Scan(&pagingData)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("could not load paging data for collection %s: %w", collectionName, err)
	}

	return pagingData, nil
}

func (r *Repository) Save(collectionName, pagingData string) error {
	if r.db == nil {
		return fmt.Errorf("repository is not open")
	}

	_, err := r.db.Exec(fmt.Sprintf("INSERT OR REPLACE INTO %s (collection_name, paging_data) VALUES (?, ?)", pagingTableName), collectionName, pagingData)
	if err != nil {
		return fmt.Errorf("could not save paging data for collection %s: %w", collectionName, err)
	}
	return nil
}
