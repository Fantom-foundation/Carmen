// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package carmen

import "github.com/Fantom-foundation/Carmen/go/common"

type MemoryFootprint interface {
	// GetChild returns a child of the memory footprint with the given name.
	GetChild(name string) MemoryFootprint

	// Value provides the amount of bytes consumed by the database structure (excluding its subcomponents)
	Value() uintptr

	// Total provides the amount of bytes consumed by the database structure including all its subcomponents
	Total() uintptr

	// ToString provides the memory footprint as a tree summary in a string
	// The name param allows to give a name to the root of the tree.
	ToString(name string) string

	// String allow memory footprints to be used in format strings.
	String() string

	// Visit iterates the footprint tree (including all subcomponents) and applies visit onto them
	Visit(visit func(footprint MemoryFootprint))
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

func (m *memoryFootprint) Visit(visit func(footprint MemoryFootprint)) {
	m.fp.Visit(func(footprint *common.MemoryFootprint) {
		visit(NewMemoryFootprint(footprint))
	})
}
