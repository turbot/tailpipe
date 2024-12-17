package config

// global map or resources with subtypes - populated at init
var resourcesWithSubtypes = map[string]struct{}{}

func registerResourceWithSubType(blockType string) {
	resourcesWithSubtypes[blockType] = struct{}{}
}

func ResourceHasSubtype(blockType string) bool {
	_, ok := resourcesWithSubtypes[blockType]
	return ok
}
