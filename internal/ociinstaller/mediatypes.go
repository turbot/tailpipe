package ociinstaller

import (
	"fmt"
	"runtime"

	"github.com/turbot/pipe-fittings/ociinstaller"
)

type TailpipeMediaTypeProvider struct{}

func (p TailpipeMediaTypeProvider) GetAllMediaTypes(imageType ociinstaller.ImageType) ([]string, error) {
	m, err := p.MediaTypeForPlatform(imageType)
	if err != nil {
		return nil, err
	}
	s := p.SharedMediaTypes(imageType)
	c := p.ConfigMediaTypes()
	return append(append(m, s...), c...), nil
}

// MediaTypeForPlatform returns media types for binaries for this OS and architecture
// and it's fallbacks in order of priority
func (TailpipeMediaTypeProvider) MediaTypeForPlatform(imageType ociinstaller.ImageType) ([]string, error) {
	layerFmtGzip := "application/vnd.turbot.tailpipe.%s.%s-%s.layer.v1+gzip"

	switch imageType {
	case ociinstaller.ImageTypePlugin:
		return []string{fmt.Sprintf(layerFmtGzip, imageType, runtime.GOOS, runtime.GOARCH)}, nil
	}
	return []string{}, nil
}

// SharedMediaTypes returns media types that are NOT specific to the os and arch (readmes, control files, etc)
func (TailpipeMediaTypeProvider) SharedMediaTypes(imageType ociinstaller.ImageType) []string {
	switch imageType {
	case ociinstaller.ImageTypePlugin:
		return []string{ociinstaller.MediaTypePluginSpcLayer(), ociinstaller.MediaTypePluginLicenseLayer()}
	}
	return nil
}

// ConfigMediaTypes :: returns media types for OCI $config data ( in the config, not a layer)
func (TailpipeMediaTypeProvider) ConfigMediaTypes() []string {
	return []string{ociinstaller.MediaTypeConfig(), ociinstaller.MediaTypePluginConfig()}
}
