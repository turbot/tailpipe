load "$LIB_BATS_ASSERT/load.bash"
load "$LIB_BATS_SUPPORT/load.bash"

@test "verify metadata in versions.json file after plugin install" {
  # Ensure chaos plugin is installed (it should already be in acceptance tests)
  run tailpipe plugin list --output json
  echo $output
  
  # Verify chaos plugin is in the list
  assert_output --partial "hub.tailpipe.io/plugins/turbot/chaos@latest"
  
  # Read the versions.json file
  versions_file="$TAILPIPE_INSTALL_DIR/plugins/versions.json"
  
  # Verify the file exists
  [ -f "$versions_file" ]
  
  # Read the file content
  versions_content=$(cat "$versions_file")
  echo "Versions file content: $versions_content"
  
  # Extract metadata for chaos plugin using jq
  chaos_plugin_key="hub.tailpipe.io/plugins/turbot/chaos@latest"
  
  # Verify that metadata exists for the chaos plugin
  metadata_exists=$(echo "$versions_content" | jq -r --arg key "$chaos_plugin_key" '.plugins | has($key) and (.[$key] | has("metadata"))')
  assert_equal "$metadata_exists" "true"
  
  # Verify tables metadata - chaos plugin should have specific tables
  tables=$(echo "$versions_content" | jq -r --arg key "$chaos_plugin_key" '.plugins[$key].metadata.tables // [] | sort | join(",")')
  assert_equal "$tables" "chaos_all_columns,chaos_date_time,chaos_struct_columns"
  
  # Verify sources metadata - chaos plugin should have specific sources  
  sources=$(echo "$versions_content" | jq -r --arg key "$chaos_plugin_key" '.plugins[$key].metadata.sources // [] | sort | join(",")')
  assert_equal "$sources" "chaos_all_columns,chaos_date_time,chaos_struct_columns"
}

@test "verify format types and presets metadata exists in versions.json file after plugin install" {
  # Read the versions.json file
  versions_file="$TAILPIPE_INSTALL_DIR/plugins/versions.json"
  
  # Verify the file exists
  [ -f "$versions_file" ]
  
  # Read the file content
  versions_content=$(cat "$versions_file")
  echo "Versions file content: $versions_content"
  
  # Test format_types and format_presets from core plugin (which has them)
  core_plugin_key="hub.tailpipe.io/plugins/turbot/core@latest"

  # Verify format_types content - should contain the expected types
  format_types=$(echo "$versions_content" | jq -r --arg key "$core_plugin_key" '.plugins[$key].metadata.format_types // [] | sort | join(",")')
  assert_equal "$format_types" "delimited,grok,jsonl,regex"

  # Verify format_presets content - should contain the expected presets
  format_presets=$(echo "$versions_content" | jq -r --arg key "$core_plugin_key" '.plugins[$key].metadata.format_presets // [] | sort | join(",")')
  assert_equal "$format_presets" "delimited.default,jsonl.default"
}