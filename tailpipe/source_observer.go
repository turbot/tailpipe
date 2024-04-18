package tailpipe

import (
	"math"

	"github.com/turbot/tailpipe-plugin-sdk/source"
)

type EventDiscoverArtifactsStart struct {
}

type EventDiscoverArtifactsProgress struct {
	Current int64
	Total   int64
}

func (e *EventDiscoverArtifactsProgress) Percentage() int {
	if e.Total == 0 {
		return 0
	}
	percentage := float64(e.Current) / float64(e.Total) * 100
	return int(math.Ceil(percentage))
}

type EventDiscoverArtifactsEnd struct {
	Error error
}

type EventArtifactDiscovered struct {
	ArtifactInfo *source.ArtifactInfo
}

type EventDownloadArtifactStart struct {
	ArtifactInfo *source.ArtifactInfo
}

type EventDownloadArtifactProgress struct {
	ArtifactInfo *source.ArtifactInfo
	Current      int64
	Total        int64
}

func (e *EventDownloadArtifactProgress) Percentage() int {
	if e.Total == 0 {
		return 0
	}
	percentage := float64(e.Current) / float64(e.Total) * 100
	return int(math.Ceil(percentage))
}

type EventDownloadArtifactEnd struct {
	ArtifactInfo *source.ArtifactInfo
	Error        error
}

type EventArtifactDownloaded struct {
	Artifact *source.Artifact
}

type EventDownloadStart struct {
}

type EventDownloadProgress struct {
	Current int64
	Total   int64
}

func (e *EventDownloadProgress) Percentage() int {
	if e.Total == 0 {
		return 0
	}
	percentage := float64(e.Current) / float64(e.Total) * 100
	return int(math.Ceil(percentage))
}

type EventDownloadEnd struct {
	Error error
}
