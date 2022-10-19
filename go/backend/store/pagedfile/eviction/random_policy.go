package eviction

type RandomPolicy struct {
	clean FlatSet
	dirty FlatSet
}

func NewRandomPolicy(capacity int) Policy {
	return &RandomPolicy{
		clean: NewFlatSet(capacity),
		dirty: NewFlatSet(capacity),
	}
}

func (re *RandomPolicy) Read(pageId int) {
	if !re.dirty.Contains(pageId) {
		re.clean.Add(pageId)
	}
}

func (re *RandomPolicy) Written(pageId int) {
	re.clean.Remove(pageId)
	re.dirty.Add(pageId)
}

func (re *RandomPolicy) Removed(pageId int) {
	re.clean.Remove(pageId)
	re.dirty.Remove(pageId)
}

func (re *RandomPolicy) GetPageToEvict() int {
	if !re.clean.IsEmpty() {
		return re.clean.PickRandom()
	}
	if !re.dirty.IsEmpty() {
		return re.dirty.PickRandom()
	}
	return -1
}
