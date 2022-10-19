package eviction

import "math/rand"

// FlatSet provides a set of unique entries collection, with possibility to pick a random entry.
// The set use a map to ensure the values uniqueness, but it combines it with a slice,
// to allow index-based access to entries, which is necessary for the random entry picking.
type FlatSet struct {
	positions map[int]int // maps items to their positions in the entries slice
	entries   []int
}

func NewFlatSet(capacity int) FlatSet {
	return FlatSet{
		positions: make(map[int]int, capacity),
		entries:   make([]int, 0, capacity),
	}
}

func (fs *FlatSet) Add(item int) {
	_, exists := fs.positions[item]
	if !exists {
		fs.positions[item] = len(fs.entries)
		fs.entries = append(fs.entries, item)
	}
}

func (fs *FlatSet) Remove(item int) {
	_, exists := fs.positions[item]
	if exists {
		lastItem := fs.entries[len(fs.entries)-1]
		if lastItem != item { // move to the end of entries
			deletedItemPosition := fs.positions[item]
			fs.entries[deletedItemPosition] = lastItem
			fs.positions[lastItem] = deletedItemPosition
		}
		fs.entries = fs.entries[0 : len(fs.entries)-1]
		delete(fs.positions, item)
	}
}

func (fs *FlatSet) Contains(item int) bool {
	_, exists := fs.positions[item]
	return exists
}

func (fs *FlatSet) IsEmpty() bool {
	return len(fs.entries) == 0
}

// PickRandom provides a random entry from the set with a uniform probability distribution.
func (fs *FlatSet) PickRandom() int {
	return fs.entries[rand.Intn(len(fs.entries))]
}
