package tailpipe

import (
	"context"
	"expvar"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/rs/xid"
	"github.com/turbot/tailpipe-plugin-sdk/collection"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type Writer interface {
	Open() error
	Write(r collection.Row) error
	Close() error
}

/*

type DuckDBWriter struct {
	db    *sql.DB
	mutex sync.Mutex
	stats *expvar.Map
}

func (w *DuckDBWriter) Open() error {
	// Empty datasource means, that DB will be solely in-memory, otherwise you could specify a filename here
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return err
	}
	w.db = db
	_, err = w.db.Exec("CREATE TABLE IF NOT EXISTS test (tp_id VARCHAR, tp_timestamp INT64);")
	w.stats = expvar.NewMap("duckdb_writer")
	w.stats.Add("writes", 0)
	return err
}

func (w *DuckDBWriter) Write(r collection.Row) error {
	// Create query to insert row fields into test table
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Check if db is nil and call Open if it is
	if w.db == nil {
		err := w.Open()
		if err != nil {
			return err
		}
	}
	query := "INSERT INTO test (tp_id, tp_timestamp) VALUES ($1, $2);"
		fmt.Println(query)
		fmt.Println("tp_id", r.GetTpID())
		fmt.Println("tp_timestamp", r.GetTpTimestamp())
	_, err := w.db.Exec(query, r.GetTpID(), r.GetTpTimestamp())
	if err != nil {
		fmt.Println("Error in query:", err)
		return err
	}
	w.stats.Add("writes", 1)

	return nil
}

func (w *DuckDBWriter) PrintStats() {
	fmt.Println("Writes: ", w.stats.Get("writes"))
}

func (w *DuckDBWriter) Close() error {
	return w.db.Close()
}

*/

type WriterManager struct {
	collection *Collection
	writers    map[string]Writer
	mutex      sync.RWMutex
}

// TODO - should the writer manager auto close writers that have been idle for a while?

func NewWriterManager(c *Collection) *WriterManager {
	return &WriterManager{
		collection: c,
		writers:    make(map[string]Writer),
	}
}

func (m *WriterManager) HivePathForRow(r collection.Row) (string, error) {
	return fmt.Sprintf("tp_collection=%s/tp_connection=%s/tp_year=%d/tp_month=%d/tp_day=%d", m.collection.Name, r.GetConnection(), r.GetYear(), r.GetMonth(), r.GetDay()), nil
}

func (m *WriterManager) OpenForRow(row collection.Row) (Writer, error) {

	hivePath, err := m.HivePathForRow(row)
	if err != nil {
		return nil, err
	}

	// Safely access the writer for this hivePath. A read lock is used to
	// check and return the write if it exists already. Otherwise, we acquire
	// a write lock and create the writer.

	// Read the writer for this hivePath. A read lock lets us check this
	// without blocking other readers.
	m.mutex.RLock()
	writer, ok := m.writers[hivePath]
	m.mutex.RUnlock()

	// If there is already a writer, return it
	if ok {
		return writer, nil
	}

	// Acquire a write lock. This will now block others who also need to create a
	// new writer. This is fine, as we only want one writer per hivePath.
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Double-check if the writer has been created by another goroutine. It's
	// possible - in fact probable when we're starting execution - that another
	// goroutine has already created the writer between when our reader couldn't
	// find it and when we acquired the write lock.
	writer, ok = m.writers[hivePath]
	if ok {
		return writer, nil
	}

	// Finally, create the writer and save for others to use.

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	opts := []ParquetWriterOption{
		WithDestinationPath(filepath.Join(homeDir, "Downloads", "tailpipe")),
		WithHivePath(hivePath),
		WithRow(m.collection.GetPlugin().Schema()),
	}

	w, err := NewParquetWriter(m.collection.ctx, opts...)
	if err != nil {
		return nil, err
	}

	err = w.Open()
	if err != nil {
		return nil, err
	}

	m.writers[hivePath] = w
	return w, nil
}

func (m *WriterManager) Write(row collection.Row) error {
	w, err := m.OpenForRow(row)
	if err != nil {
		return err
	}
	return w.Write(row)
}

func (m *WriterManager) Close() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for hivePath, w := range m.writers {
		if err := w.Close(); err != nil {
			return err
		}
		delete(m.writers, hivePath)
	}
	return nil
}

type ParquetWriter struct {
	DestinationPath string
	HivePath        string
	Schema          collection.Row

	RowGroupSize       int64
	PageSize           int64
	MarshalConcurrency int64
	CompressionType    parquet.CompressionCodec
	AutoCloseTimeout   int64

	ctx    context.Context
	handle *writer.ParquetWriter
	mutex  sync.Mutex

	autoCloseTimer  *time.Timer
	cancelAutoClose context.CancelFunc

	stats *expvar.Map
}

type ParquetWriterOption func(*ParquetWriter) error

func NewParquetWriter(ctx context.Context, opts ...ParquetWriterOption) (*ParquetWriter, error) {

	// Set defaults
	w := &ParquetWriter{
		ctx: ctx,

		// Defaults from https://github.com/apache/parquet-format?tab=readme-ov-file#configurations
		RowGroupSize:       64 * 1024 * 1024,
		PageSize:           256 * 1024,
		MarshalConcurrency: 4,
		CompressionType:    parquet.CompressionCodec_SNAPPY,

		// Automatically close writers after 30 seconds of inactivity
		AutoCloseTimeout: 30000,
	}

	// Load options
	for _, opt := range opts {
		if err := opt(w); err != nil {
			return nil, err
		}
	}

	// Reset stats
	w.stats = expvar.NewMap(fmt.Sprintf("parquet_writer:%s", w.HivePath))
	w.stats.Add("writes", 0)

	return w, nil
}

func WithDestinationPath(path string) ParquetWriterOption {
	return func(w *ParquetWriter) error {
		w.DestinationPath = path
		return nil
	}
}

func WithHivePath(path string) ParquetWriterOption {
	return func(w *ParquetWriter) error {
		w.HivePath = path
		return nil
	}
}

func WithRow(r collection.Row) ParquetWriterOption {
	return func(w *ParquetWriter) error {
		w.Schema = r
		return nil
	}
}

func (w *ParquetWriter) Open() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// If it doesn't exist, create a new one.
	// TODO - make this secure and safe
	outputFile := fmt.Sprintf("%s/%s/%s.parquet", w.DestinationPath, w.HivePath, xid.New().String())

	fmt.Println("Create new writer: ", time.Now().String(), outputFile)

	// Create the directory if it doesn't exist
	err := os.MkdirAll(filepath.Dir(outputFile), 0755)
	if err != nil {
		return err
	}

	fw, err := local.NewLocalFileWriter(outputFile)
	if err != nil {
		return err
	}

	pw, err := writer.NewParquetWriter(fw, w.Schema, w.MarshalConcurrency)
	if err != nil {
		return err
	}

	w.handle = pw

	w.resetAutoCloseTimer()

	return nil
}

func (w *ParquetWriter) Write(r collection.Row) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	err := w.handle.Write(r)
	if err != nil {
		return err
	}
	w.stats.Add("writes", 1)
	w.resetAutoCloseTimer()
	return nil
}

func (w *ParquetWriter) Close() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if w.autoCloseTimer != nil {
		w.autoCloseTimer.Stop()
	}
	if w.cancelAutoClose != nil {
		w.cancelAutoClose()
	}
	fmt.Println("Close writer: ", time.Now().String(), w.HivePath, w.stats.Get("writes"))
	return w.handle.WriteStop()
}

func (w *ParquetWriter) resetAutoCloseTimer() {
	if w.autoCloseTimer != nil {
		w.autoCloseTimer.Stop()
	}
	var ctx context.Context
	ctx, w.cancelAutoClose = context.WithCancel(w.ctx)
	d := time.Duration(w.AutoCloseTimeout) * time.Millisecond
	w.autoCloseTimer = time.AfterFunc(d, func() {
		select {
		case <-ctx.Done():
			// Context was canceled, possibly because a new write came in
			return
		default:
			// Timer expired without any writes, close the writer
			fmt.Println("Auto-closing writer due to inactivity.", w.HivePath)
			w.Close()
		}
	})
}
