package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"bufio"
	"runtime"
	"sync"

	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/plugin"
)

func (c *Collector) doCollectSynthetic(ctx context.Context, fromTime time.Time, toTime time.Time, overwrite bool) (*plugin.CollectResponse, error) {
	// create the execution
	// NOTE: create _before_ calling the plugin to ensure it is ready to receive the started event
	c.execution = &execution{
		id:             "synthetic",
		partition:      c.partition.UnqualifiedName,
		table:          c.partition.TableName,
		plugin:         "synthetic",
		state:          ExecutionState_PENDING,
		completionChan: make(chan error, 1),
	}

	schema := buildsyntheticchema(c.partition.SyntheticMetadata.Columns)
	// start a thread to fake the collection process
	go c.collectSynthetic(ctx, schema, fromTime, toTime)

	// build a collect response
	collectResponse := &plugin.CollectResponse{
		ExecutionId: c.execution.id,
		Schema:      schema,
		FromTime: &row_source.ResolvedFromTime{
			Time:   fromTime,
			Source: "synthetic",
		},
	}
	// _now_ set the execution id
	c.execution.id = collectResponse.ExecutionId
	return collectResponse, nil
}

// syntheticColumnTypes defines the available column types for synthetic data generation
var syntheticColumnTypes = []struct {
	Name         string
	SQLType      string
	StructFields []*schema.ColumnSchema
}{
	{"string_col", "VARCHAR", nil},
	{"int_col", "INTEGER", nil},
	{"float_col", "DOUBLE", nil},
	{"bool_col", "BOOLEAN", nil},
	{"json_col", "JSON", nil},
	{"timestamp_col", "TIMESTAMP", nil},
	{"array_col", "JSON", nil},
	{"nested_json_col", "JSON", nil},
	{"uuid_col", "VARCHAR", nil},
	{"simple_struct_col", "STRUCT", []*schema.ColumnSchema{
		{
			SourceName:  "id",
			ColumnName:  "id",
			Type:        "INTEGER",
			Description: "Simple struct ID field",
		},
		{
			SourceName:  "name",
			ColumnName:  "name",
			Type:        "VARCHAR",
			Description: "Simple struct name field",
		},
		{
			SourceName:  "active",
			ColumnName:  "active",
			Type:        "BOOLEAN",
			Description: "Simple struct active field",
		},
	}},
	{"nested_struct_col", "STRUCT", []*schema.ColumnSchema{
		{
			SourceName: "metadata",
			ColumnName: "metadata",
			Type:       "STRUCT",
			StructFields: []*schema.ColumnSchema{
				{
					SourceName:  "created_at",
					ColumnName:  "created_at",
					Type:        "VARCHAR",
					Description: "Creation timestamp",
				},
				{
					SourceName:  "version",
					ColumnName:  "version",
					Type:        "VARCHAR",
					Description: "Version string",
				},
			},
			Description: "Metadata information",
		},
		{
			SourceName: "data",
			ColumnName: "data",
			Type:       "STRUCT",
			StructFields: []*schema.ColumnSchema{
				{
					SourceName:  "field1",
					ColumnName:  "field1",
					Type:        "INTEGER",
					Description: "Numeric field 1",
				},
				{
					SourceName:  "field2",
					ColumnName:  "field2",
					Type:        "VARCHAR",
					Description: "String field 2",
				},
				{
					SourceName:  "field3",
					ColumnName:  "field3",
					Type:        "BOOLEAN",
					Description: "Boolean field 3",
				},
			},
			Description: "Data fields",
		},
	}},
	{"complex_struct_col", "STRUCT", []*schema.ColumnSchema{
		{
			SourceName: "user",
			ColumnName: "user",
			Type:       "STRUCT",
			StructFields: []*schema.ColumnSchema{
				{
					SourceName:  "id",
					ColumnName:  "id",
					Type:        "INTEGER",
					Description: "User ID",
				},
				{
					SourceName:  "name",
					ColumnName:  "name",
					Type:        "VARCHAR",
					Description: "User name",
				},
				{
					SourceName: "profile",
					ColumnName: "profile",
					Type:       "STRUCT",
					StructFields: []*schema.ColumnSchema{
						{
							SourceName:  "age",
							ColumnName:  "age",
							Type:        "INTEGER",
							Description: "User age",
						},
						{
							SourceName:  "email",
							ColumnName:  "email",
							Type:        "VARCHAR",
							Description: "User email",
						},
						{
							SourceName:  "verified",
							ColumnName:  "verified",
							Type:        "BOOLEAN",
							Description: "Email verified",
						},
					},
					Description: "User profile information",
				},
			},
			Description: "User information",
		},
		{
			SourceName: "settings",
			ColumnName: "settings",
			Type:       "STRUCT",
			StructFields: []*schema.ColumnSchema{
				{
					SourceName:  "theme",
					ColumnName:  "theme",
					Type:        "VARCHAR",
					Description: "UI theme",
				},
				{
					SourceName:  "notifications",
					ColumnName:  "notifications",
					Type:        "BOOLEAN",
					Description: "Notifications enabled",
				},
			},
			Description: "User settings",
		},
	}},
}

// ConcurrentDataGenerator handles concurrent data generation and marshaling
type ConcurrentDataGenerator struct {
	numWorkers int
	rowChan    chan []byte
	errorChan  chan error
	doneChan   chan bool
}

// NewConcurrentDataGenerator creates a new concurrent data generator
func NewConcurrentDataGenerator(numWorkers int) *ConcurrentDataGenerator {
	return &ConcurrentDataGenerator{
		numWorkers: numWorkers,
		rowChan:    make(chan []byte, numWorkers*100), // Buffer for generated rows
		errorChan:  make(chan error, 1),
		doneChan:   make(chan bool, 1),
	}
}

// generateRowData generates a single row's JSON data
func generateRowData(rowIndex int, partition *config.Partition, tableSchema *schema.TableSchema, fromTime time.Time, timestampInterval time.Duration) ([]byte, error) {
	// Create row map
	rowMap := make(map[string]any, len(tableSchema.Columns))
	timestamp := fromTime.Add(time.Duration(rowIndex) * timestampInterval).Format("2006-01-02 15:04:05")

	// Populate row map (skip tp_index and tp_date)
	for _, column := range tableSchema.Columns {
		if column.ColumnName == "tp_index" || column.ColumnName == "tp_date" {
			continue
		}

		switch column.ColumnName {
		case "tp_timestamp":
			rowMap[column.ColumnName] = timestamp
		case "tp_partition":
			rowMap[column.ColumnName] = partition.ShortName
		case "tp_table":
			rowMap[column.ColumnName] = partition.TableName
		default:
			// Generate synthetic data for other columns
			rowMap[column.ColumnName] = generateSyntheticValue(column, rowIndex)
		}
	}

	// Marshal to JSON
	data, err := json.Marshal(rowMap)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal row %d: %w", rowIndex, err)
	}

	// Add newline
	data = append(data, '\n')
	return data, nil
}

// worker generates data for a range of rows
func (cdg *ConcurrentDataGenerator) worker(startRow, endRow int, partition *config.Partition, tableSchema *schema.TableSchema, fromTime time.Time, timestampInterval time.Duration) {
	for rowIndex := startRow; rowIndex < endRow; rowIndex++ {
		data, err := generateRowData(rowIndex, partition, tableSchema, fromTime, timestampInterval)
		if err != nil {
			select {
			case cdg.errorChan <- err:
			default:
			}
			return
		}

		select {
		case cdg.rowChan <- data:
		case <-cdg.doneChan:
			return
		}
	}
}

// writeOptimizedChunkToJSONLConcurrent uses multiple goroutines for data generation
func writeOptimizedChunkToJSONLConcurrent(filepath string, tableSchema *schema.TableSchema, rows int, startRowIndex int, partition *config.Partition, fromTime time.Time, timestampInterval time.Duration) error {
	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filepath, err)
	}
	defer file.Close()

	// Use buffered writer for better I/O performance
	bufWriter := bufio.NewWriter(file)
	defer bufWriter.Flush()

	// Determine number of workers (use CPU cores, but cap at reasonable number)
	numWorkers := runtime.NumCPU()
	if numWorkers > 8 {
		numWorkers = 8 // Cap at 8 to avoid too much overhead
	}
	if numWorkers > rows {
		numWorkers = rows // Don't create more workers than rows
	}

	// Create concurrent data generator
	cdg := NewConcurrentDataGenerator(numWorkers)

	// Calculate rows per worker
	rowsPerWorker := rows / numWorkers
	remainder := rows % numWorkers

	// Start workers
	var wg sync.WaitGroup
	startRow := startRowIndex
	for i := 0; i < numWorkers; i++ {
		endRow := startRow + rowsPerWorker
		if i < remainder {
			endRow++ // Distribute remainder rows
		}

		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			cdg.worker(start, end, partition, tableSchema, fromTime, timestampInterval)
		}(startRow, endRow)

		startRow = endRow
	}

	// Start a goroutine to close the row channel when all workers are done
	go func() {
		wg.Wait()
		close(cdg.rowChan)
	}()

	// Write rows from channel to file
	rowsWritten := 0
	for data := range cdg.rowChan {
		if _, err := bufWriter.Write(data); err != nil {
			close(cdg.doneChan) // Signal workers to stop
			return fmt.Errorf("failed to write row %d: %w", rowsWritten, err)
		}
		rowsWritten++
	}

	// Check for errors
	select {
	case err := <-cdg.errorChan:
		return fmt.Errorf("worker error: %w", err)
	default:
	}

	if rowsWritten != rows {
		return fmt.Errorf("expected %d rows, but wrote %d", rows, rowsWritten)
	}

	return nil
}

func buildsyntheticchema(columns int) *schema.TableSchema {
	// Create a basic schema with the required number of columns
	// Start with required tp_ fields
	s := &schema.TableSchema{
		Columns: make([]*schema.ColumnSchema, 0, columns+5), // +5 for tp_ fields (including tp_index and tp_date)
	}

	// Add required tp_ fields first
	tpFields := []struct {
		name        string
		columnType  string
		description string
	}{
		{"tp_timestamp", "TIMESTAMP", "Timestamp when the record was collected"},
		{"tp_partition", "VARCHAR", "Partition identifier"},
		{"tp_table", "VARCHAR", "Table identifier"},
		{"tp_index", "VARCHAR", "Index identifier"},
		{"tp_date", "VARCHAR", "Date identifier"},
	}

	for _, tpField := range tpFields {
		column := &schema.ColumnSchema{
			SourceName:   tpField.name,
			ColumnName:   tpField.name,
			Type:         tpField.columnType,
			StructFields: nil,
			Description:  tpField.description,
			Required:     true, // tp_ fields are always required
			NullIf:       "",
			Transform:    "",
		}
		s.Columns = append(s.Columns, column)
	}

	// Add the specified number of synthetic columns by cycling through the column types
	for i := 0; i < columns; i++ {
		// Cycle through the column types
		typeIndex := i % len(syntheticColumnTypes)
		baseType := syntheticColumnTypes[typeIndex]

		// Create a unique column name
		columnName := fmt.Sprintf("%s_%d", baseType.Name, i)

		column := &schema.ColumnSchema{
			SourceName:   columnName,
			ColumnName:   columnName,
			Type:         baseType.SQLType,
			StructFields: baseType.StructFields,
			Description:  fmt.Sprintf("Synthetic column of type %s", baseType.SQLType),
			Required:     false,
			NullIf:       "",
			Transform:    "",
		}

		s.Columns = append(s.Columns, column)
	}

	return s
}

func (c *Collector) collectSynthetic(ctx context.Context, tableSchema *schema.TableSchema, fromTime time.Time, toTime time.Time) {
	metadata := c.partition.SyntheticMetadata

	// set the execution state to started
	c.execution.state = ExecutionState_STARTED

	c.Notify(ctx, &events.Started{ExecutionId: c.execution.id})

	var chunkIdx int32 = 0
	var totalRowsProcessed int64 = 0

	// Calculate timestamp interval based on fromTime, toTime, and total rows
	var timestampInterval time.Duration
	if metadata.Rows > 1 {
		timestampInterval = toTime.Sub(fromTime) / time.Duration(metadata.Rows-1)
	} else {
		timestampInterval = 0
	}

	for rowCount := 0; rowCount < metadata.Rows; rowCount += metadata.ChunkSize {
		t := time.Now()
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			c.execution.completionChan <- ctx.Err()
			return
		default:
		}

		rows := int(math.Min(float64(metadata.Rows-rowCount), float64(metadata.ChunkSize)))

		// write optimized chunk to JSONL file
		filename := table.ExecutionIdToJsonlFileName(c.execution.id, chunkIdx)
		filepath := filepath.Join(c.sourcePath, filename)

		// write the chunk to JSONL file using optimized approach
		if err := writeOptimizedChunkToJSONLConcurrent(filepath, tableSchema, rows, rowCount, c.partition, fromTime, timestampInterval); err != nil {
			c.execution.completionChan <- fmt.Errorf("error writing chunk to JSONL file: %w", err)
			return
		}

		dur := time.Since(t)
		// if this is less that deliver interval, wait for the remaining time
		if metadata.DeliveryIntervalMs > 0 && dur < time.Duration(metadata.DeliveryIntervalMs)*time.Millisecond {
			slog.Debug("Waiting for delivery interval", "duration", dur, "expected", time.Duration(metadata.DeliveryIntervalMs)*time.Millisecond)
			select {
			case <-time.After(time.Duration(metadata.DeliveryIntervalMs)*time.Millisecond - dur):
			case <-ctx.Done():
				c.execution.completionChan <- ctx.Err()
				return
			}
		}
		// send chunk event to the plugin
		c.Notify(ctx, &events.Chunk{
			ExecutionId: c.execution.id,
			ChunkNumber: chunkIdx,
		})

		totalRowsProcessed += int64(rows)
		c.Notify(ctx, &events.Status{
			ExecutionId:  c.execution.id,
			RowsReceived: totalRowsProcessed,
			RowsEnriched: totalRowsProcessed,
		})

		chunkIdx++
	}

	// Send completion event
	c.Notify(ctx, events.NewCompletedEvent(c.execution.id, int64(metadata.Rows), chunkIdx, nil))

	// Signal completion
	c.execution.completionChan <- nil
}

func generateSyntheticValue(column *schema.ColumnSchema, rowIndex int) any {
	// Use the column's Type field directly instead of fuzzy matching on name
	columnType := column.Type

	// Generate value based on exact type match (case-insensitive)
	switch strings.ToUpper(columnType) {
	case "VARCHAR":
		return fmt.Sprintf("%s_val%d", column.ColumnName, rowIndex%100000)
	case "INTEGER":
		return (rowIndex % 100000) + 1
	case "DOUBLE":
		return float64(rowIndex%100000) * 0.1
	case "BOOLEAN":
		return rowIndex%2 == 0
	case "JSON":
		return generateJSONValue(column, rowIndex)
	case "TIMESTAMP":
		return time.Now().AddDate(0, 0, -rowIndex%30).Format("2006-01-02 15:04:05")
	default:
		// Handle struct types and complex types
		if strings.Contains(strings.ToUpper(columnType), "STRUCT") {
			return generateStructValue(column, rowIndex)
		}
		// For any other unrecognized type, throw an error
		panic(fmt.Sprintf("Unsupported column type '%s' for column '%s'", columnType, column.ColumnName))
	}
}

func generateJSONValue(column *schema.ColumnSchema, rowIndex int) any {
	// Generate different JSON structures based on column name
	if strings.Contains(column.ColumnName, "nested_json") {
		return map[string]any{
			"metadata": map[string]any{
				"created_at": time.Now().AddDate(0, 0, -rowIndex%30).Format("2006-01-02"),
				"version":    fmt.Sprintf("v%d.%d", rowIndex%10, rowIndex%5),
			},
			"data": map[string]any{
				"field1": rowIndex % 100000,
				"field2": fmt.Sprintf("field_%d", rowIndex%100000),
				"field3": rowIndex%2 == 0,
			},
		}
	} else if strings.Contains(column.ColumnName, "array") {
		return []any{
			fmt.Sprintf("item_%d", rowIndex%100000),
			rowIndex % 100000,
			rowIndex%2 == 0,
			float64(rowIndex%100000) * 0.1,
		}
	} else {
		// Default JSON object
		return map[string]any{
			"id":    rowIndex % 100000,
			"name":  fmt.Sprintf("item_%d", rowIndex%100000),
			"value": (rowIndex % 100000) + 1,
			"tags":  []string{"tag1", "tag2", "tag3"},
		}
	}
}

func generateStructValue(column *schema.ColumnSchema, rowIndex int) any {
	if column.StructFields == nil {
		return map[string]any{
			"id":   rowIndex % 100000,
			"name": fmt.Sprintf("struct_%d", rowIndex%100000),
		}
	}

	result := make(map[string]any)
	for _, field := range column.StructFields {
		if field.StructFields != nil {
			// Nested struct
			result[field.ColumnName] = generateStructValue(field, rowIndex)
		} else {
			// Simple field
			result[field.ColumnName] = generateSyntheticValue(field, rowIndex)
		}
	}
	return result
}

// writeOptimizedChunkToJSONL implements an optimized approach for faster JSONL writing
// It uses buffered I/O and direct marshaling for better performance
func writeOptimizedChunkToJSONL(filepath string, tableSchema *schema.TableSchema, rows int, startRowIndex int, partition *config.Partition, fromTime time.Time, timestampInterval time.Duration) error {
	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filepath, err)
	}
	defer file.Close()

	// Use buffered writer for better I/O performance
	bufWriter := bufio.NewWriter(file)
	defer bufWriter.Flush()

	// Pre-allocate the row map to avoid repeated allocations
	rowMap := make(map[string]any, len(tableSchema.Columns))

	// Write each row
	for i := 0; i < rows; i++ {
		rowIndex := startRowIndex + i
		timestamp := fromTime.Add(time.Duration(rowIndex) * timestampInterval).Format("2006-01-02 15:04:05")

		// Clear the map for reuse
		for k := range rowMap {
			delete(rowMap, k)
		}

		// Populate row map (skip tp_index and tp_date)
		for _, column := range tableSchema.Columns {
			if column.ColumnName == "tp_index" || column.ColumnName == "tp_date" {
				continue
			}

			switch column.ColumnName {
			case "tp_timestamp":
				rowMap[column.ColumnName] = timestamp
			case "tp_partition":
				rowMap[column.ColumnName] = partition.ShortName
			case "tp_table":
				rowMap[column.ColumnName] = partition.TableName
			default:
				// Generate synthetic data for other columns
				rowMap[column.ColumnName] = generateSyntheticValue(column, rowIndex)
			}
		}

		// Marshal to bytes and write directly
		data, err := json.Marshal(rowMap)
		if err != nil {
			return fmt.Errorf("failed to marshal row %d: %w", rowIndex, err)
		}

		if _, err := bufWriter.Write(data); err != nil {
			return fmt.Errorf("failed to write row %d: %w", rowIndex, err)
		}
		if _, err := bufWriter.Write([]byte{'\n'}); err != nil {
			return fmt.Errorf("failed to write newline for row %d: %w", rowIndex, err)
		}
	}

	return nil
}
