package query

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/pipe-fittings/export"
	"github.com/turbot/pipe-fittings/modconfig"
)

func queryExporters() []export.Exporter {
	// TODO #snapshot
	return nil
	//return []export.Exporter{&export.SnapshotExporter{}}
}

// getQueriesFromArgs retrieves queries from args
//
// For each arg check if it is a named query or a file, before falling back to treating it as sql
func getQueriesFromArgs(args []string) ([]*modconfig.ResolvedQuery, error) {

	var queries = make([]*modconfig.ResolvedQuery, len(args))
	for idx, arg := range args {
		resolvedQuery, err := ResolveQueryAndArgsFromSQLString(arg)
		if err != nil {
			return nil, err
		}
		if len(resolvedQuery.ExecuteSQL) > 0 {
			// default name to the query text
			resolvedQuery.Name = resolvedQuery.ExecuteSQL

			queries[idx] = resolvedQuery
		}
	}
	return queries, nil
}

// ResolveQueryAndArgsFromSQLString attempts to resolve 'arg' to a query and query args
func ResolveQueryAndArgsFromSQLString(sqlString string) (*modconfig.ResolvedQuery, error) {
	var err error

	// 2) is this a file
	// get absolute filename
	filePath, err := filepath.Abs(sqlString)
	if err != nil {
		return nil, fmt.Errorf("%s", err.Error())
	}
	fileQuery, fileExists, err := getQueryFromFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("%s", err.Error())
	}
	if fileExists {
		if fileQuery.ExecuteSQL == "" {
			error_helpers.ShowWarning(fmt.Sprintf("file '%s' does not contain any data", filePath))
			// (just return the empty query - it will be filtered above)
		}
		return fileQuery, nil
	}
	// the argument cannot be resolved as an existing file
	// if it has a sql suffix (i.e we believe the user meant to specify a file) return a file not found error
	if strings.HasSuffix(strings.ToLower(sqlString), ".sql") {
		return nil, fmt.Errorf("file '%s' does not exist", filePath)
	}

	// 2) just use the query string as is and assume it is valid SQL
	return &modconfig.ResolvedQuery{RawSQL: sqlString, ExecuteSQL: sqlString}, nil
}

// try to treat the input string as a file name and if it exists, return its contents
func getQueryFromFile(input string) (*modconfig.ResolvedQuery, bool, error) {
	// get absolute filename
	path, err := filepath.Abs(input)
	if err != nil {
		//nolint:golint,nilerr // if this gives any error, return not exist
		return nil, false, nil
	}

	// does it exist?
	if _, err := os.Stat(path); err != nil {
		//nolint:golint,nilerr // if this gives any error, return not exist (we may get a not found or a path too long for example)
		return nil, false, nil
	}

	// read file
	fileBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, true, err
	}

	res := &modconfig.ResolvedQuery{
		RawSQL:     string(fileBytes),
		ExecuteSQL: string(fileBytes),
	}
	return res, true, nil
}
