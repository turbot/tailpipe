package display

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/turbot/pipe-fittings/printers"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
)

type PartitionResource struct {
	Name        string       `json:"name"`
	Description *string      `json:"description,omitempty"`
	Plugin      string       `json:"plugin"`
	Local       FileMetadata `json:"local,omitempty"`
}

// GetShowData implements the printers.Showable interface
func (r *PartitionResource) GetShowData() *printers.RowData {
	res := printers.NewRowData(
		printers.NewFieldValue("Name", r.Name),
		printers.NewFieldValue("Description", r.Description),
		printers.NewFieldValue("Plugin", r.Plugin),
		printers.NewFieldValue("Local Size", r.Local.HumanizeSize()),
		printers.NewFieldValue("Local Files", r.Local.HumanizeCount()),
	)
	return res
}

// GetListData implements the printers.Listable interface
func (r *PartitionResource) GetListData() *printers.RowData {
	res := printers.NewRowData(
		printers.NewFieldValue("NAME", r.Name),
		printers.NewFieldValue("PLUGIN", r.Plugin),
		printers.NewFieldValue("LOCAL SIZE", r.Local.HumanizeSize()),
		printers.NewFieldValue("FILES", r.Local.HumanizeCount()),
	)
	return res
}

func ListPartitionResources(ctx context.Context) ([]*PartitionResource, error) {
	var res []*PartitionResource

	partitions := config.GlobalConfig.Partitions
	for _, p := range partitions {
		name := fmt.Sprintf("%s.%s", p.Table, p.ShortName)
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

func GetPartitionResource(ctx context.Context, partitionName string) (*PartitionResource, error) {
	partitions := config.GlobalConfig.Partitions
	for _, p := range partitions {
		name := fmt.Sprintf("%s.%s", p.Table, p.ShortName)
		if name == partitionName {
			partition := &PartitionResource{
				Name:        name,
				Description: p.Description,
				Plugin:      p.Plugin.Alias,
			}

			err := partition.setFileInformation()
			if err != nil {
				return nil, fmt.Errorf("error setting file information: %w", err)
			}

			return partition, nil
		}
	}

	return nil, fmt.Errorf("partition '%s' not found", partitionName)
}

func (r *PartitionResource) setFileInformation() error {
	dataDir := config.GlobalWorkspaceProfile.GetDataDir()

	nameParts := strings.Split(r.Name, ".")
	tableDir := fmt.Sprintf("%s=%s", constants.TpTable, nameParts[0])
	partitionDir := fmt.Sprintf("%s=%s", constants.TpPartition, nameParts[1])

	metadata, err := getFileMetadata(path.Join(dataDir, tableDir, partitionDir))
	if err != nil {
		return err
	}

	r.Local = metadata

	return nil
}
