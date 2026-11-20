package database

import (
	"context"
	"fmt"
	"os"

	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/tailpipe/internal/config"
)

// partitionTable identifies a partition which has data in the database
type partitionTable struct {
	table     string
	partition string
}

// getPartitionsWithData returns every table/partition pair which has at least one live data file
func getPartitionsWithData(ctx context.Context, db *DuckDb) ([]partitionTable, error) {
	// the first partition key of a tailpipe table is tp_partition (see partitionKey)
	query := fmt.Sprintf(`select t.table_name, fpv.partition_value
from %[1]s.ducklake_data_file df
	join %[1]s.ducklake_file_partition_value fpv on df.data_file_id = fpv.data_file_id and fpv.partition_key_index = 0
	join %[1]s.ducklake_table t on df.table_id = t.table_id
where df.end_snapshot is null
group by t.table_name, fpv.partition_value
order by t.table_name, fpv.partition_value`, pconstants.DuckLakeMetadataCatalog)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list partitions with data: %w", err)
	}
	defer rows.Close()

	var res []partitionTable
	for rows.Next() {
		var pt partitionTable
		if err := rows.Scan(&pt.table, &pt.partition); err != nil {
			return nil, fmt.Errorf("failed to scan partition: %w", err)
		}
		res = append(res, pt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over partitions: %w", err)
	}
	return res, nil
}

// GetUnconfiguredPartitions returns the partitions which have data in the database but are not in the given
// config, keyed by unqualified name (<table>.<partition>).
func GetUnconfiguredPartitions(ctx context.Context, db *DuckDb, cfg *config.TailpipeConfig) (map[string]*config.Partition, error) {
	withData, err := getPartitionsWithData(ctx, db)
	if err != nil {
		return nil, err
	}

	res := make(map[string]*config.Partition)
	for _, pt := range withData {
		name := fmt.Sprintf("%s.%s", pt.table, pt.partition)
		if _, ok := cfg.Partitions[name]; ok {
			continue
		}
		res[name] = config.NewUnconfiguredPartition(pt.table, pt.partition, cfg)
	}
	return res, nil
}

// GetAllPartitions returns the configured partitions together with the unconfigured ones found in the database,
// keyed by unqualified name
func GetAllPartitions(ctx context.Context, db *DuckDb, cfg *config.TailpipeConfig) (map[string]*config.Partition, error) {
	unconfigured, err := GetUnconfiguredPartitions(ctx, db, cfg)
	if err != nil {
		return nil, err
	}
	res := make(map[string]*config.Partition, len(cfg.Partitions)+len(unconfigured))
	for k, v := range cfg.Partitions {
		res[k] = v
	}
	for k, v := range unconfigured {
		res[k] = v
	}
	return res, nil
}

// ReadUnconfiguredPartitions is GetUnconfiguredPartitions for a caller which has no database connection:
// it opens a read only one for the call. If there is no DuckLake database there is no data, so it returns nothing
// rather than creating one.
func ReadUnconfiguredPartitions(ctx context.Context, cfg *config.TailpipeConfig) (map[string]*config.Partition, error) {
	if _, err := os.Stat(config.GlobalWorkspaceProfile.GetDucklakeDbPath()); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	db, err := NewDuckDb(WithDuckLakeReadonly())
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return GetUnconfiguredPartitions(ctx, db, cfg)
}
