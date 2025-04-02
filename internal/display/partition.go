package display

import (
	"context"
	"fmt"
	"strings"

	"github.com/turbot/pipe-fittings/v2/printers"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/filepaths"
)

// PartitionResource represents a partition resource and is used for list/show commands
type PartitionResource struct {
	Name        string             `json:"name"`
	Description *string            `json:"description,omitempty"`
	Plugin      string             `json:"plugin"`
	Local       TableResourceFiles `json:"local,omitempty"`
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

func ListPartitionResources(ctx context.Context) ([]*PartitionResource, error) {
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
		name := fmt.Sprintf("%s.%s", p.TableName, p.ShortName)
		partition := &PartitionResource{
			Name:        name,
			Description: p.Description,
			Plugin:      p.Plugin.Alias,
		}

		err := partition.setFileInformation()
		if err != nil {
			return nil, fmt.Errorf("error setting file information: %w", err)
		}

		res = append(res, partition)
	}

	return res, nil
}

func GetPartitionResource(partitionName string) (*PartitionResource, error) {
	p, ok := config.GlobalConfig.Partitions[partitionName]
	if !ok {
		return nil, fmt.Errorf("no partitions found")
	}
	partition := &PartitionResource{
		Name:        partitionName,
		Description: p.Description,
		Plugin:      p.Plugin.Alias,
	}

	err := partition.setFileInformation()
	if err != nil {
		return nil, fmt.Errorf("error setting file information: %w", err)
	}

	return partition, nil
}

func (r *PartitionResource) setFileInformation() error {
	dataDir := config.GlobalWorkspaceProfile.GetDataDir()

	nameParts := strings.Split(r.Name, ".")

	partitionDir := filepaths.GetParquetPartitionPath(dataDir, nameParts[0], nameParts[1])
	metadata, err := getFileMetadata(partitionDir)
	if err != nil {
		return err
	}

	r.Local.FileMetadata = metadata

	if metadata.FileCount > 0 {
		var rc int64
		rc, err = database.GetRowCount(context.Background(), nameParts[0], &nameParts[1])
		if err != nil {
			return fmt.Errorf("unable to obtain row count: %w", err)
		}
		r.Local.RowCount = rc
	}

	return nil
}
