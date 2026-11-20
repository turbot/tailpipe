package display

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	"github.com/turbot/pipe-fittings/v2/printers"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
	error_helpers "github.com/turbot/tailpipe/internal/error_helpers"
)

// PartitionResource represents a partition resource and is used for list/show commands
type PartitionResource struct {
	Name        string  `json:"name"`
	Description *string `json:"description,omitempty"`
	Plugin      string  `json:"plugin"`
	// Configured is false for a partition which has data but is no longer in the configuration
	Configured bool               `json:"configured"`
	Local      TableResourceFiles `json:"local,omitempty"`
	table      string
	partition  string
}

func NewPartitionResource(p *config.Partition) *PartitionResource {
	return &PartitionResource{
		Name:        p.UnqualifiedName,
		Description: p.Description,
		Plugin:      p.Plugin.Alias,
		Configured:  !p.Unconfigured,
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
		printers.NewFieldValue("Configured", configuredText(r.Configured)),
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
		printers.NewFieldValue("CONFIGURED", configuredText(r.Configured)),
		printers.NewFieldValue("LOCAL SIZE", humanizeBytes(r.Local.FileSize)),
		printers.NewFieldValue("FILES", humanizeCount(r.Local.FileCount)),
		printers.NewFieldValue("ROWS", humanizeCount(r.Local.RowCount)),
	)
	return res
}

// ListPartitionResources lists the configured partitions and any which have data but are no longer configured
func ListPartitionResources(ctx context.Context, db *database.DuckDb) ([]*PartitionResource, error) {
	var res []*PartitionResource

	// add in the partitions which exist in the database but not in the config
	partitions, err := allPartitionsFromDb(ctx, db)
	if err != nil {
		return nil, err
	}

	for _, p := range partitions {
		partition := NewPartitionResource(p)

		// populate the partition resource with local file information
		err := partition.setFileInformation(ctx, db)
		if err != nil {
			return nil, fmt.Errorf("error setting file information: %w", err)
		}

		res = append(res, partition)
	}

	sort.Slice(res, func(i, j int) bool { return res[i].Name < res[j].Name })

	return res, nil
}

// allPartitions returns the configured partitions plus any which have data but are no longer configured.
// It is used by commands which do not otherwise need the database (plugin list/show): if the database cannot be
// read, it warns and returns the configured partitions only, rather than failing the command.
func allPartitions(ctx context.Context) []*config.Partition {
	configured := config.GlobalConfig.Partitions
	res := make([]*config.Partition, 0, len(configured))
	for _, p := range configured {
		res = append(res, p)
	}

	unconfigured, err := database.ReadUnconfiguredPartitions(ctx, config.GlobalConfig.Partitions, config.GlobalConfig.PluginVersions)
	if err != nil {
		slog.Warn("failed to read unconfigured partitions", "error", err)
		error_helpers.ShowWarning(fmt.Sprintf("could not read the partitions in the database, so partitions no longer in the configuration are not shown: %s", err.Error()))
		return res
	}
	for _, p := range unconfigured {
		res = append(res, p)
	}
	return res
}

// allPartitionsFromDb returns the configured partitions plus any which have data in db but are no longer configured
func allPartitionsFromDb(ctx context.Context, db *database.DuckDb) ([]*config.Partition, error) {
	partitions, err := database.GetAllPartitions(ctx, db, config.GlobalConfig.Partitions, config.GlobalConfig.PluginVersions)
	if err != nil {
		return nil, fmt.Errorf("error listing partitions: %w", err)
	}
	res := make([]*config.Partition, 0, len(partitions))
	for _, p := range partitions {
		res = append(res, p)
	}
	return res, nil
}

func configuredText(configured bool) string {
	if configured {
		return "yes"
	}
	return "no"
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
	metadata, err := database.GetPartitionFileMetadata(ctx, r.table, r.partition, db)
	if err != nil {
		return fmt.Errorf("unable to obtain file metadata: %w", err)
	}

	r.Local.FileSize = metadata.FileSize
	r.Local.FileCount = metadata.FileCount
	r.Local.RowCount = metadata.RowCount

	return nil
}
