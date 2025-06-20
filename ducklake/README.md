# DuckLake - Hello World Test App

A simple Go application that demonstrates basic DuckDB functionality.

## Features

- Connect to DuckDB in-memory database
- Create tables and insert data
- Query data with SQL
- Test DuckDB-specific features (JSON, arrays, dates)

## Prerequisites

- Go 1.21 or later
- DuckDB Go driver

## Installation

1. Navigate to the ducklake directory:
   ```bash
   cd ducklake
   ```

2. Initialize the Go module and download dependencies:
   ```bash
   go mod tidy
   ```

3. Run the application:
   ```bash
   go run main.go
   ```

## Expected Output

The application will:
- Connect to DuckDB
- Create a `hello_world` table
- Insert test data
- Query and display the results
- Test various DuckDB features

## Dependencies

- `github.com/marcboeker/go-duckdb` - DuckDB Go driver

## Notes

This is a test application to verify DuckDB functionality in a Go environment. The database runs in-memory, so data is not persisted between runs. 