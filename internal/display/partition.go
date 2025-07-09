package display

import (
	"context"
	"fmt"
	"github.com/turbot/pipe-fittings/v2/printers"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/parquet"
)

// PartitionResource represents a partition resource and is used for list/show commands
type PartitionResource struct {
	Name        string             `json:"name"`
	Description *string            `json:"description,omitempty"`
	Plugin      string             `json:"plugin"`
	Local       TableResourceFiles `json:"local,omitempty"`
	table       string
	partition   string
}

func NewPartitionResource(p *config.Partition) *PartitionResource {
	return &PartitionResource{
		Name:        p.UnqualifiedName,
		Description: p.Description,
		Plugin:      p.Plugin.Alias,
		table:       p.TableName,
		partition:   p.ShortName,
	}
}

// GetShowData implements the printers.Showable interface
func (r *PartitionResource) GetShowData() *printers.RowData {
	res := printers.NewRowData(
		printers.NewFieldValue("Name", r.Name),
		printers.NewFieldValue("Description", r.Description),
		printers.NewFieldValue("Plugin", r.Plugin),
		printers.NewFieldValue("Local Size", humanizeBytes(r.Local.FileSize)),
		printers.NewFieldValue("Local Files", humanizeBytes(r.Local.FileCount)),
	)
	return res
}

// GetListData implements the printers.Listable interface
func (r *PartitionResource) GetListData() *printers.RowData {
	res := printers.NewRowData(
		printers.NewFieldValue("NAME", r.Name),
		printers.NewFieldValue("PLUGIN", r.Plugin),
		printers.NewFieldValue("LOCAL SIZE", humanizeBytes(r.Local.FileSize)),
		printers.NewFieldValue("FILES", humanizeCount(r.Local.FileCount)),
		printers.NewFieldValue("ROWS", humanizeCount(r.Local.RowCount)),
	)
	return res
}

func ListPartitionResources(ctx context.Context, db *database.DuckDb) ([]*PartitionResource, error) {
	var res []*PartitionResource

	// TODO Add in unconfigured partitions to list output
	// load all partition names from the data
	//partitionNames, err := database.ListPartitions(ctx)
	//if err != nil {
	//	return nil, fmt.Errorf("error listing partitions: %w", err)
	//}
	//fmt.Println(partitionNames)

	partitions := config.GlobalConfig.Partitions
	for _, p := range partitions {
		partition := NewPartitionResource(p)

		// populate the partition resource with local file information
		err := partition.setFileInformation(ctx, db)
		if err != nil {
			return nil, fmt.Errorf("error setting file information: %w", err)
		}

		res = append(res, partition)
	}

	return res, nil
}

func GetPartitionResource(ctx context.Context, p *config.Partition, db *database.DuckDb) (*PartitionResource, error) {
	partition := NewPartitionResource(p)

	err := partition.setFileInformation(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("error setting file information: %w", err)
	}

	return partition, nil
}

func (r *PartitionResource) setFileInformation(ctx context.Context, db *database.DuckDb) error {

	// Get file metadata using shared function
	metadata, err := parquet.GetPartitionFileMetadata(ctx, r.table, r.partition, db)
	if err != nil {
		return fmt.Errorf("unable to obtain file metadata: %w", err)
	}

	r.Local.FileSize = metadata.FileSize
	r.Local.FileCount = metadata.FileCount
	r.Local.RowCount = metadata.RowCount

	return nil
}
