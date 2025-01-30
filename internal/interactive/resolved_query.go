package interactive

// ResolvedQuery contains the execute SQL and flag indicating if the query is a meta query
type ResolvedQuery struct {
	ExecuteSQL  string
	IsMetaQuery bool
}
