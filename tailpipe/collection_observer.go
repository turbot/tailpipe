package tailpipe

import (
	"github.com/turbot/tailpipe-plugin-sdk/collection"
	"github.com/turbot/tailpipe-plugin-sdk/source"
)

type EventExtractRowsStart struct {
	Artifact *source.Artifact
}

type EventRow struct {
	Artifact *source.Artifact
	Row      collection.Row
}

type EventExtractRowsEnd struct {
	Artifact *source.Artifact
	Error    error
}

type EventSyncArtifactStart struct {
	Artifact *source.Artifact
}

type EventSyncArtifactEnd struct {
	Artifact *source.Artifact
	Error    error
}
