package main

import (
	"regexp"
)

type CachedMatchers struct {
	ConcurrentMap[ConcurrentMap[bool]]
}

func NewCachedMatchers() CachedMatchers {
	return CachedMatchers{NewConcurrentMap[ConcurrentMap[bool]]()}
}

func (c *CachedMatchers) cachedMatchAny(v, regexpMatchString string, callback func(string) bool) bool {
	var cachedMatch *bool
	matcherCache := c.Get(regexpMatchString)
	if matcherCache != nil {
		cachedMatch = matcherCache.Get(v)

		if cachedMatch != nil {
			return *cachedMatch
		}
	} else {
		newCache := NewConcurrentMap[bool]()
		matcherCache = &newCache
		c.Set(regexpMatchString, &newCache)
	}

	matched := callback(v)
	matcherCache.Set(v, &matched)

	return matched
}

func (c *CachedMatchers) MemoizedRegexpsMatch(s string, rs []*regexp.Regexp) bool {
	for _, r := range rs {
		if c.cachedMatchAny(s, r.String(), r.MatchString) {
			return true
		}
	}

	return false
}
