package database

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	pf "github.com/turbot/pipe-fittings/v2/filepaths"
	"github.com/turbot/pipe-fittings/v2/versionfile"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
)

// newTestDb returns an in-memory database which owns its extension and temp directories
func newTestDb(t *testing.T) *DuckDb {
	t.Helper()
	// NewDuckDb needs an install dir for its extensions
	orig := pf.PipesInstallDir
	pf.PipesInstallDir = t.TempDir()
	t.Cleanup(func() { pf.PipesInstallDir = orig })

	db, err := NewDuckDb(WithTempDir(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// newMetadataTestDb returns a database with the DuckLake metadata tables the partition queries read,
// attached under the name DuckLake gives its metadata catalog. It stands in for a real DuckLake, which needs
// extensions to be installed: the queries under test only read these three tables.
func newMetadataTestDb(t *testing.T) *DuckDb {
	t.Helper()
	db := newTestDb(t)

	cat := pconstants.DuckLakeMetadataCatalog
	for _, stmt := range []string{
		fmt.Sprintf("attach ':memory:' as %s", cat),
		fmt.Sprintf("create table %s.ducklake_table (table_id bigint, table_name varchar)", cat),
		fmt.Sprintf("create table %s.ducklake_data_file (data_file_id bigint, table_id bigint, end_snapshot bigint)", cat),
		fmt.Sprintf("create table %s.ducklake_file_partition_value (data_file_id bigint, table_id bigint, partition_key_index bigint, partition_value varchar)", cat),
	} {
		_, err := db.Exec(stmt)
		require.NoError(t, err, stmt)
	}
	return db
}

// addTable adds a table to the test metadata
func addTable(t *testing.T, db *DuckDb, tableID int, name string) {
	t.Helper()
	_, err := db.Exec(fmt.Sprintf("insert into %s.ducklake_table values (?, ?)", pconstants.DuckLakeMetadataCatalog), tableID, name)
	require.NoError(t, err)
}

// addFile adds a data file for the given table, with the given tp_partition and tp_index partition values.
// endSnapshot is nil for a live file
func addFile(t *testing.T, db *DuckDb, fileID, tableID int, partition, index string, endSnapshot any) {
	t.Helper()
	cat := pconstants.DuckLakeMetadataCatalog
	_, err := db.Exec(fmt.Sprintf("insert into %s.ducklake_data_file values (?, ?, ?)", cat), fileID, tableID, endSnapshot)
	require.NoError(t, err)
	for i, v := range []string{partition, index} {
		_, err = db.Exec(fmt.Sprintf("insert into %s.ducklake_file_partition_value values (?, ?, ?, ?)", cat), fileID, tableID, i, v)
		require.NoError(t, err)
	}
}

// testConfig returns a config with the given plugin versions and the given configured (table, partition) pairs
func testConfig(versions map[string]*versionfile.InstalledVersion, partitions ...[2]string) *config.TailpipeConfig {
	cfg := config.NewTailpipeConfig()
	cfg.PluginVersions = versions
	for _, n := range partitions {
		p := &config.Partition{TableName: n[0]}
		p.ShortName = n[1]
		p.UnqualifiedName = n[0] + "." + n[1]
		cfg.Partitions[p.UnqualifiedName] = p
	}
	return cfg
}

func TestGetUnconfiguredPartitions(t *testing.T) {
	ctx := context.Background()
	versions := map[string]*versionfile.InstalledVersion{
		"hub.tailpipe.io/plugins/turbot/aws@latest": {Metadata: map[string][]string{"tables": {"aws_cloudtrail_log"}}},
	}

	t.Run("a partition with data and no config is returned", func(t *testing.T) {
		db := newMetadataTestDb(t)
		addTable(t, db, 1, "aws_cloudtrail_log")
		addFile(t, db, 1, 1, "kept", "default", nil)
		addFile(t, db, 2, 1, "removed", "default", nil)
		// a second file in the same partition must not repeat it
		addFile(t, db, 3, 1, "removed", "default", nil)

		got, err := GetUnconfiguredPartitions(ctx, db, testConfig(versions, [2]string{"aws_cloudtrail_log", "kept"}))
		require.NoError(t, err)
		require.Len(t, got, 1)

		p := got["aws_cloudtrail_log.removed"]
		require.NotNil(t, p)
		require.True(t, p.Unconfigured)
		require.Equal(t, "aws_cloudtrail_log", p.TableName)
		require.Equal(t, "removed", p.ShortName)
		require.Equal(t, "aws_cloudtrail_log.removed", p.UnqualifiedName)
		require.Equal(t, "partition.aws_cloudtrail_log.removed", p.FullName)
		// the plugin is found from the table
		require.Equal(t, "hub.tailpipe.io/plugins/turbot/aws@latest", p.Plugin.Plugin)
	})

	t.Run("a configured partition is not returned", func(t *testing.T) {
		db := newMetadataTestDb(t)
		addTable(t, db, 1, "aws_cloudtrail_log")
		addFile(t, db, 1, 1, "kept", "default", nil)

		got, err := GetUnconfiguredPartitions(ctx, db, testConfig(versions, [2]string{"aws_cloudtrail_log", "kept"}))
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("a partition whose files are all expired is not returned", func(t *testing.T) {
		db := newMetadataTestDb(t)
		addTable(t, db, 1, "aws_cloudtrail_log")
		addFile(t, db, 1, 1, "deleted", "default", 7)

		got, err := GetUnconfiguredPartitions(ctx, db, testConfig(versions))
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("a tp_index which looks like a partition name is not a partition", func(t *testing.T) {
		db := newMetadataTestDb(t)
		addTable(t, db, 1, "aws_cloudtrail_log")
		addFile(t, db, 1, 1, "kept", "other_name", nil)

		got, err := GetUnconfiguredPartitions(ctx, db, testConfig(versions, [2]string{"aws_cloudtrail_log", "kept"}))
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("the same partition name in two tables is two partitions", func(t *testing.T) {
		db := newMetadataTestDb(t)
		addTable(t, db, 1, "aws_cloudtrail_log")
		addTable(t, db, 2, "gcp_audit_log")
		addFile(t, db, 1, 1, "p1", "default", nil)
		addFile(t, db, 2, 2, "p1", "default", nil)

		got, err := GetUnconfiguredPartitions(ctx, db, testConfig(versions, [2]string{"aws_cloudtrail_log", "p1"}))
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.NotNil(t, got["gcp_audit_log.p1"])
	})

	t.Run("a partition of a custom table is a core plugin partition", func(t *testing.T) {
		db := newMetadataTestDb(t)
		addTable(t, db, 1, "my_logs")
		addFile(t, db, 1, 1, "removed", "default", nil)

		cfg := testConfig(versions)
		cfg.CustomTables["my_logs"] = &config.Table{}
		got, err := GetUnconfiguredPartitions(ctx, db, cfg)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, constants.CorePluginInstallStream(), got["my_logs.removed"].Plugin.Alias)
		require.NotNil(t, got["my_logs.removed"].CustomTable)
	})

	t.Run("no data at all", func(t *testing.T) {
		db := newMetadataTestDb(t)
		got, err := GetUnconfiguredPartitions(ctx, db, testConfig(versions))
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("a metadata error is returned, not hidden", func(t *testing.T) {
		// no metadata catalog attached
		db := newTestDb(t)

		_, err := GetUnconfiguredPartitions(ctx, db, testConfig(versions))
		require.Error(t, err)
	})
}

func TestGetAllPartitions(t *testing.T) {
	db := newMetadataTestDb(t)
	addTable(t, db, 1, "aws_cloudtrail_log")
	addFile(t, db, 1, 1, "removed", "default", nil)

	cfg := testConfig(nil, [2]string{"aws_cloudtrail_log", "kept"})
	got, err := GetAllPartitions(context.Background(), db, cfg)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.False(t, got["aws_cloudtrail_log.kept"].Unconfigured)
	require.True(t, got["aws_cloudtrail_log.removed"].Unconfigured)
	// the configured map is not modified
	require.Len(t, cfg.Partitions, 1)
}
