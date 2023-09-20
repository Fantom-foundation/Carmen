package stock

import "golang.org/x/exp/constraints"

// ComplementSet is a set implementation for integer values in particular
// suitable for large sets of continuous elements with few missing elements.
type ComplementSet[I constraints.Integer] struct {
	lowerBound I // inclusive
	upperBound I // exclusive
	excluded   map[I]struct{}
}

// MakeComplementSet creates a set containing the elements in the interval [lowerBound, upperBound).
func MakeComplementSet[I constraints.Integer](lowerBound, upperBound I) *ComplementSet[I] {
	return &ComplementSet[I]{
		lowerBound: lowerBound,
		upperBound: upperBound,
	}
}

func (s *ComplementSet[I]) GetLowerBound() I {
	return s.lowerBound
}

func (s *ComplementSet[I]) GetUpperBound() I {
	return s.upperBound
}

func (s *ComplementSet[I]) Contains(i I) bool {
	if i < s.lowerBound || i >= s.upperBound {
		return false
	}
	_, excluded := s.excluded[i]
	return !excluded
}

func (s *ComplementSet[I]) Remove(i I) {
	if i < s.lowerBound || i >= s.upperBound {
		return
	}
	if s.excluded == nil {
		s.excluded = map[I]struct{}{}
	}
	s.excluded[i] = struct{}{}
}
