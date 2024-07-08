package carmen

import "github.com/Fantom-foundation/Carmen/go/common"

type MemoryFootprint interface {
	// GetChild returns a child of the memory footprint with the given name.
	GetChild(name string) MemoryFootprint

	// AddChild allows to attach a new value under current memory footprint.
	AddChild(name string, childValue uintptr)

	// SetNote allows to attach a string comment to the memory report
	SetNote(note string)

	// Value provides the amount of bytes consumed by the database structure (excluding its subcomponents)
	Value() uintptr

	// Total provides the amount of bytes consumed by the database structure including all its subcomponents
	Total() uintptr

	// ToString provides the memory footprint as a tree summary in a string
	// The name param allows to give a name to the root of the tree.
	ToString(name string) string

	// String allow memory footprints to be used in format strings.
	String() string
}

func NewMemoryFootprint(fp *common.MemoryFootprint) MemoryFootprint {
	return &memoryFootprint{
		fp: fp,
	}
}

func NewMemoryFootprintFromValue(value uintptr) MemoryFootprint {
	return &memoryFootprint{
		fp: common.NewMemoryFootprint(value),
	}
}

// memoryFootprint describes the memory consumption of a database structure.
type memoryFootprint struct {
	fp *common.MemoryFootprint
}

func (m *memoryFootprint) GetChild(name string) MemoryFootprint {
	return &memoryFootprint{m.fp.GetChild(name)}
}

func (m *memoryFootprint) AddChild(name string, childValue uintptr) {
	m.fp.AddChild(name, common.NewMemoryFootprint(childValue))
}

func (m *memoryFootprint) SetNote(note string) {
	m.fp.SetNote(note)
}

func (m *memoryFootprint) Value() uintptr {
	return m.fp.Value()
}

func (m *memoryFootprint) Total() uintptr {
	return m.fp.Total()
}

func (m *memoryFootprint) ToString(name string) string {
	return m.fp.ToString(name)
}

func (m *memoryFootprint) String() string {
	return m.fp.String()
}
