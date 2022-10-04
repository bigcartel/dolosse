package main

type Set[T comparable] map[T]struct{}

func (s *Set[T]) Contains(n T) bool {
	_, ok := (*s)[n]
	return ok
}

func (s *Set[T]) Add(n T) {
	(*s)[n] = struct{}{}
}
