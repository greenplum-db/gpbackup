package utils

/*
 * This file contains an implementation of a set as a wrapper around a map[string]bool
 * for use in filtering lists.
 */

/*
 * This set implementation can be used in one of two ways.  An "include" set
 * returns true if an item is in the map and false otherwise, while an "exclude"
 * set returns false if an item is in the map and true otherwise, so that the
 * set can be used for filtering on items in lists.
 *
 * The alwaysMatchesFilter variable causes MatchesFilter() to always return true
 * if an empty list is passed, so that it doesn't attempt to filter on anything
 * The isExclude variable controls whether a set is an include set or an exclude
 * set.
 */
type FilterSet struct {
	Set                 map[string]bool
	IsExclude           bool
	AlwaysMatchesFilter bool
}

func NewSet(list []string) *FilterSet {
	newSet := FilterSet{}
	newSet.Set = make(map[string]bool, len(list))
	for _, item := range list {
		newSet.Set[item] = true
	}
	return &newSet
}

func NewIncludeSet(list []string) *FilterSet {
	newSet := NewSet(list)
	(*newSet).AlwaysMatchesFilter = len(list) == 0
	return newSet
}

func NewExcludeSet(list []string) *FilterSet {
	newSet := NewSet(list)
	(*newSet).AlwaysMatchesFilter = len(list) == 0
	(*newSet).IsExclude = true
	return newSet
}

func (s *FilterSet) MatchesFilter(item string) bool {
	if s.AlwaysMatchesFilter {
		return true
	}
	_, matches := s.Set[item]
	if s.IsExclude {
		return !matches
	}
	return matches
}

func (s *FilterSet) Length() int {
	return len(s.Set)
}

func (s *FilterSet) Equals(s1 *FilterSet) bool {
	if s.Length() != s1.Length() {
		return false
	}

	for k := range s.Set {
		if _, ok := s1.Set[k]; !ok {
			return false
		}
	}
	return true
}
