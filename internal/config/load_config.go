package config

var GlobalConfig *TailpipeConfig

//type loadConfigOptions struct {
//	include        []string
//	allowedOptions []string
//}
//
//func loadConfig(ctx context.Context, configFolder string, TailpipeConfig *TailpipeConfig, opts *loadConfigOptions) error_helpers.ErrorAndWarnings {
//	log.Printf("[INFO] loadConfig is loading connection config")
//	// get all the config files in the directory
//	configPaths, err := filehelpers.ListFilesWithContext(ctx, configFolder, &filehelpers.ListOptions{
//		Flags:   filehelpers.FilesFlat,
//		Include: opts.include,
//	})
//
//	if err != nil {
//		log.Printf("[WARN] loadConfig: failed to get config file paths: %v\n", err)
//		return error_helpers.NewErrorsAndWarning(err)
//	}
//	if len(configPaths) == 0 {
//		return error_helpers.ErrorAndWarnings{}
//	}
//
//	fileData, diags := pparse.LoadFileData(configPaths...)
//	if diags.HasErrors() {
//		log.Printf("[WARN] loadConfig: failed to load all config files: %v\n", err)
//		return error_helpers.DiagsToErrorsAndWarnings("Failed to load all config files", diags)
//	}
//
//	body, diags := pparse.ParseHclFiles(fileData)
//	if diags.HasErrors() {
//		return error_helpers.DiagsToErrorsAndWarnings("Failed to load all config files", diags)
//	}
//
//	// do a partial decode
//	content, moreDiags := body.Content(pparse.ConfigBlockSchema)
//	if moreDiags.HasErrors() {
//		diags = append(diags, moreDiags...)
//		return error_helpers.DiagsToErrorsAndWarnings("Failed to load config", diags)
//	}
//
//	// store block types which we have found in this folder - each is only allowed once
//	// NOTE this is different to merging options with options already populated in the passed-in steampipe config
//	// this is valid because the same block may be defined in the config folder and the workspace
//	optionBlockMap := map[string]bool{}
//
//	for _, block := range content.Blocks {
//		switch block.Type {
//
//		case modconfig.BlockTypePlugin:
//			plugin, moreDiags := pparse.DecodePlugin(block)
//			diags = append(diags, moreDiags...)
//			if moreDiags.HasErrors() {
//				continue
//			}
//			// add plugin to TailpipeConfig
//			// NOTE: this errors if there is a plugin block with a duplicate label
//			if err := TailpipeConfig.addPlugin(plugin); err != nil {
//				return error_helpers.NewErrorsAndWarning(err)
//			}
//
//		case modconfig.BlockTypeConnection:
//			connection, moreDiags := pparse.DecodeConnection(block)
//			diags = append(diags, moreDiags...)
//			if moreDiags.HasErrors() {
//				continue
//			}
//			if existingConnection, alreadyThere := TailpipeConfig.Connections[connection.Name]; alreadyThere {
//				err := getDuplicateConnectionError(existingConnection, connection)
//				return error_helpers.NewErrorsAndWarning(err)
//			}
//			if ok, errorMessage := db_common.IsSchemaNameValid(connection.Name); !ok {
//				return error_helpers.NewErrorsAndWarning(sperr.New("invalid connection name: '%s' in '%s'. %s ", connection.Name, block.TypeRange.Filename, errorMessage))
//			}
//			TailpipeConfig.Connections[connection.Name] = connection
//
//
//	}
//
//	if diags.HasErrors() {
//		return error_helpers.DiagsToErrorsAndWarnings("Failed to load config", diags)
//	}
//
//	res := error_helpers.DiagsToErrorsAndWarnings("", diags)
//
//	log.Printf("[INFO] loadConfig calling initializePlugins")
//
//	// resolve the plugins for each connection and create default plugin config
//	// for all plugins mentioned in connection config which have no explicit config
//	TailpipeConfig.initializePlugins()
//
//	return res
//}
//
//
//
