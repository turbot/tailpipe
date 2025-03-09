# Tailpipe Command Data Sources

This document describes where the data for each of the List and Show commands comes from in Tailpipe.

## Plugin Commands

### `plugin list`
- Data is obtained by scanning the local filesystem for installed plugins in the plugins directory
- Uses `plugin.List()` which:
  - Scans for `.plugin` files in the plugins directory
  - Retrieves version information from the plugin versions file
  - No GRPC calls required
- Additional partition information is obtained from the global config (`config.GlobalConfig.Partitions`)

### `plugin show <plugin>`
- Makes a GRPC call to the plugin using `pluginManager.Describe()`
- Returns:
  - Plugin name and version
  - List of available sources
  - List of available tables
  - List of partitions (from global config)

## Table Commands

### `table list`
- Requires GRPC calls to each installed plugin to get their table schemas
- For each plugin:
  - Makes a `Describe()` GRPC call to get table definitions
  - Gets table schema, description, and column information
- Additional data comes from:
  - Local file information (size, count)
  - Partition information from global config
  - Row counts from the database

### `table show <table>`
- Makes a GRPC call to the specific plugin that owns the table
- Returns:
  - Table schema and description
  - Column definitions
  - Local file information
  - Partition information from global config

## Source Commands

### `source list`
- Requires GRPC calls to each installed plugin
- For each plugin:
  - Makes a `Describe()` GRPC call to get source definitions
  - Gets source name and description
- No additional configuration required

### `source show <source>`
- Gets data from the same source as `source list`
- Filters the full source list to show details for the specific source
- No additional GRPC calls required if the data is already cached

## Partition Commands

### `partition list`
- Primary data comes from global config (`config.GlobalConfig.Partitions`)
- Additional data from:
  - Local file system (file sizes and counts)
  - Database (row counts)
- No GRPC calls required

### `partition show <partition>`
- Data comes from global config for the specific partition
- Additional data from:
  - Local file system (file sizes and counts)
  - Database (row counts)
- No GRPC calls required

## Summary

- **GRPC Calls Required For**:
  - Plugin show
  - Table list/show
  - Source list/show

- **Configuration Based**:
  - Plugin list (partially)
  - Partition list/show

- **Local Filesystem**:
  - File sizes and counts for tables and partitions
  - Plugin installation information

- **Database**:
  - Row counts for tables and partitions 