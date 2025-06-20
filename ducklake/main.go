package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/marcboeker/go-duckdb/v2"
)

func main() {
	fmt.Println("🐤 DuckLake Hello World Test App")
	fmt.Println("==================================")

	// Open a new DuckDB database in memory (or file if you want persistence)
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	// 1. Install sqlite extension
	_, err = db.Exec("INSTALL sqlite;")
	if err != nil {
		log.Fatalf("Failed to INSTALL sqlite extension: %v", err)
	}
	fmt.Println("✅ Installed sqlite extension")

	_, err = db.Exec("FORCE INSTALL ducklake FROM core_nightly;")
	if err != nil {
		log.Fatalf("Failed to INSTALL ducklake nightly extension: %v", err)
	}
	fmt.Println("✅ Installed ducklake nightly extension")

	// 2. Attach the sqlite database as my_ducklake
	_, err = db.Exec("ATTACH 'ducklake:sqlite:metadata.sqlite' AS my_ducklake (DATA_PATH 'data_files/');")
	if err != nil {
		log.Fatalf("Failed to ATTACH sqlite database: %v", err)
	}
	fmt.Println("✅ Attached sqlite database as my_ducklake")

	// 3. Use the attached database
	_, err = db.Exec("USE my_ducklake;")
	if err != nil {
		log.Fatalf("Failed to USE my_ducklake: %v", err)
	}
	fmt.Println("✅ Using my_ducklake database")

	fmt.Println("\n🎉 DuckLake extension/attach test completed successfully!")
}
