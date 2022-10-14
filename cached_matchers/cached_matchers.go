package cached_matchers

import (
	"bigcartel/dolosse/concurrent_map"
	"regexp"
)

type CachedMatchers struct {
	concurrent_map.ConcurrentMap[concurrent_map.ConcurrentMap[bool]]
}

func NewCachedMatchers() CachedMatchers {
	return CachedMatchers{concurrent_map.NewConcurrentMap[concurrent_map.ConcurrentMap[bool]]()}
}

func (c CachedMatchers) cachedMatchAny(v, regexpMatchString string, callback func(string) bool) bool {
	var cachedMatch *bool
	matcherCache := c.Get(regexpMatchString)
	if matcherCache != nil {
		cachedMatch = matcherCache.Get(v)

		if cachedMatch != nil {
			return *cachedMatch
		}
	} else {
		newCache := concurrent_map.NewConcurrentMap[bool]()
		matcherCache = &newCache
		c.Set(regexpMatchString, &newCache)
	}

	matched := callback(v)
	matcherCache.Set(v, &matched)

	return matched
}

func (c CachedMatchers) MemoizedRegexpsMatch(s string, rs []*regexp.Regexp) bool {
	for _, r := range rs {
		if c.cachedMatchAny(s, r.String(), r.MatchString) {
			return true
		}
	}

	return false
}
