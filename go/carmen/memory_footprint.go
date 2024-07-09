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
	// Total provides the amount of bytes consumed by the database structure including all its subcomponents
	Total() uint64

	// String allow memory footprints to be used in format strings.
	// It returns a breakdown of all MemoryFootprints within the tree.
	String() string
}

func newMemoryFootprint(fp *common.MemoryFootprint) MemoryFootprint {
	return &memoryFootprint{
		fp: fp,
	}
}

func NewMemoryFootprintFromValue(value uint64) MemoryFootprint {
	return &memoryFootprint{
		fp: common.NewMemoryFootprint(uintptr(value)),
	}
}

// memoryFootprint describes the memory consumption of a database structure.
type memoryFootprint struct {
	fp *common.MemoryFootprint
}

func (m *memoryFootprint) Total() uint64 {
	return uint64(m.fp.Total())
}

func (m *memoryFootprint) String() string {
	return m.fp.String()
}
