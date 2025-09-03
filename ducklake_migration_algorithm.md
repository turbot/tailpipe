
# DuckLake Migration Algorithm

This document outlines the core algorithm used to migrate legacy Tailpipe data into the new DuckLake-backed storage system. The process ensures safety, reliability, and resumability, while maintaining a full backup of the original data.

---

## Overview

The migration consists of five main stages:

1. **Detect whether migration is needed**
2. **Prepare migration directories**
3. **Identify tables and schemas**
4. **Execute migration**
5. **Finalize and clean up**

---

## 1. Detect Whether Migration is Needed

Tailpipe checks whether legacy data needs to be migrated by inspecting known data locations.

### Check locations:

- `~/.tailpipe/data/`
- `~/.tailpipe/migration/migrating/`

### What to look for:

- Presence of a `tailpipe.db` file in either directory.

### Why:

- If `tailpipe.db` exists, it indicates the presence of legacy data that has not yet been migrated.
- If `tailpipe.db` exists in `~/.tailpipe/migration/migrating/`, it may indicate an **interrupted or cancelled** migration that needs to resume.

---

## 2. Prepare Migration Directories

If migration is required:

- Get the current data directory path.
- Move the **entire contents** of the data directory into:
  ```
  ~/.tailpipe/migration/migrating/
  ```

- Ensure directory structure is preserved for consistency.

---

## 3. Identify Tables and Schemas

Once the `tailpipe.db` file is detected, Tailpipe must identify which tables to migrate and the schemas associated with them.

- Open the `tailpipe.db` file.
- **Query existing DuckDB views** to extract:
  - Table names
  - Schema definitions

> Some data (e.g., partially collected files) may not have associated views. These should be **flagged** and may optionally be copied to the backup directory without migration. This is a known edge case.

---

## 4. Execute Migration

With data and schema definitions ready, Tailpipe initiates the actual migration process.

### Migration Flow:

1. **Traverse** the `~/.tailpipe/migration/migrating/` directory:
    - Each dir under `~/.tailpipe/migration/migrating/default/` should be of pattern:  
      `tp_table=<table_name>/tp_partition=.../tp_index=.../tp_date=...`
2. Match directories with known tables from the schema map.
3. For each matching leaf node (contains `.parquet` files):
    - Start a **transaction**.
    - Read the `.parquet` files.
    - **INSERT** data into the DuckLake database.
        - Schema can be taken from the earlier step or inferred from the files.
        - Validate that the inferred schema matches the expected one.
    - If insert is successful:
        - **Commit** the transaction.
        - **Move** the leaf node to `~/.tailpipe/migration/migrated/`.
    - If insert fails:
        - **Rollback** the transaction.
        - Leave the node in `migrating/` for retry.

> Each leaf node is treated as an atomic unit. Partial migrations are never committed.

---

## 5. Finalize and Clean Up

After all nodes are successfully migrated:

- Move the `tailpipe.db` file from:
  ```
  ~/.tailpipe/migration/migrating/
  ```
  to:
  ```
  ~/.tailpipe/migration/migrated/
  ```

### Expected Outcome:

- `~/.tailpipe/migration/migrating/` directory is **completely empty**.
- A full backup of original data resides in `~/.tailpipe/migration/migrated/`.
- The new DuckLake database is fully populated.

---

## Resuming Interrupted Migrations

- If migration is interrupted (e.g., crash, cancel), the `tailpipe.db` remains in `~/.tailpipe/migration/migrating/`.
- This ensures that on the next Tailpipe invocation, the detection logic re-triggers migration to resume from where it left off.

---

This algorithm provides a robust and user-safe path for transitioning to the DuckLake database backend without requiring data recollection.
