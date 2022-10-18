package eviction

import "github.com/Fantom-foundation/Carmen/go/common"

type RandomEvictionPolicy struct {
	clean common.FlatSet
	dirty common.FlatSet
}

func NewRandomEvictionPolicy(capacity int) Policy {
	return &RandomEvictionPolicy{
		clean: common.NewFlatSet(capacity),
		dirty: common.NewFlatSet(capacity),
	}
}

func (re *RandomEvictionPolicy) Read(pageId int) {
	if !re.dirty.Contains(pageId) {
		re.clean.Add(pageId)
	}
}

func (re *RandomEvictionPolicy) Written(pageId int) {
	re.clean.Remove(pageId)
	re.dirty.Add(pageId)
}

func (re *RandomEvictionPolicy) Removed(pageId int) {
	re.clean.Remove(pageId)
	re.dirty.Remove(pageId)
}

func (re *RandomEvictionPolicy) GetPageToEvict() int {
	if !re.clean.IsEmpty() {
		return re.clean.PickRandom()
	}
	if !re.dirty.IsEmpty() {
		return re.dirty.PickRandom()
	}
	return -1
}
