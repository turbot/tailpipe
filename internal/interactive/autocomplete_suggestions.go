package interactive

type autoCompleteSuggestions struct {
	//schemas            []prompt.Suggest
	//tables  []prompt.Suggest
	//tablesBySchema     map[string][]prompt.Suggest

}

func newAutocompleteSuggestions() *autoCompleteSuggestions {
	return &autoCompleteSuggestions{
		//tablesBySchema: make(map[string][]prompt.Suggest),
	}
}
func (s autoCompleteSuggestions) sort() {
	//sortSuggestions := func(s []prompt.Suggest) {
	//	sort.Slice(s, func(i, j int) bool {
	//		return s[i].Text < s[j].Text
	//	})
	//}

	//sortSuggestions(s.schemas)
	//sortSuggestions(s.tables)
	//for _, tables := range s.tablesBySchema {
	//	sortSuggestions(tables)
	//}

}
