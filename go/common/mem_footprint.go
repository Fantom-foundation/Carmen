// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package common

import (
	"fmt"
	"sort"
	"strings"
)

// MemoryFootprint describes the memory consumption of a database structure
type MemoryFootprint struct {
	value    uintptr
	children map[string]*MemoryFootprint
	note     string
}

// NewMemoryFootprint creates a new MemoryFootprint instance for a database structure
func NewMemoryFootprint(value uintptr) *MemoryFootprint {
	return &MemoryFootprint{
		value:    value,
		children: make(map[string]*MemoryFootprint),
	}
}

// SetNote allows to attach a string comment to the memory report
func (mf *MemoryFootprint) SetNote(note string) {
	mf.note = note
}

// AddChild allows to attach a MemoryFootprint of the database structure subcomponent
func (mf *MemoryFootprint) AddChild(name string, child *MemoryFootprint) {
	mf.children[name] = child
}

// GetChild returns a child of the memory footprint with the given name
func (mf *MemoryFootprint) GetChild(name string) (child *MemoryFootprint) {
	child = mf.children[name]
	return
}

// Value provides the amount of bytes consumed by the database structure (excluding its subcomponents)
func (mf *MemoryFootprint) Value() uintptr {
	return mf.value
}

// Total provides the amount of bytes consumed by the database structure including all its subcomponents
func (mf *MemoryFootprint) Total() uintptr {
	includedObjects := make(map[*MemoryFootprint]bool)
	return includeObjectIntoTotal(mf, includedObjects)
}

func includeObjectIntoTotal(mf *MemoryFootprint, includedObjects map[*MemoryFootprint]bool) (total uintptr) {
	if mf == nil {
		return 0
	}
	if _, exists := includedObjects[mf]; exists {
		return 0
	}
	includedObjects[mf] = true
	total = mf.value
	for _, child := range mf.children {
		total += includeObjectIntoTotal(child, includedObjects)
	}
	return total
}

// ToString provides the memory footprint as a tree summary in a string
// The name param allows to give a name to the root of the tree.
func (mf *MemoryFootprint) ToString(name string) string {
	var sb strings.Builder
	mf.toStringBuilder(&sb, name)
	return sb.String()
}

// Allow memory footprints to be used in format strings.
func (mf *MemoryFootprint) String() string {
	return mf.ToString(".")
}

func (mf *MemoryFootprint) toStringBuilder(sb *strings.Builder, path string) {
	// Print children in order for simpler comparison.
	names := make([]string, 0, len(mf.children))
	for name := range mf.children {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool { return names[i] < names[j] })

	for _, name := range names {
		footprint := mf.children[name]
		fullPath := path + "/" + name
		footprint.toStringBuilder(sb, fullPath)
	}

	// Show sum at the bottom.
	memoryAmountToString(sb, mf.Total())
	sb.WriteRune(' ')
	sb.WriteString(path)
	if len(mf.note) != 0 {
		sb.WriteRune(' ')
		sb.WriteString(mf.note)
	}
	sb.WriteRune('\n')
}

func memoryAmountToString(sb *strings.Builder, bytes uintptr) {
	const unit = 1024
	const prefixes = " KMGTPE"
	div, exp := 1, 0
	for n := bytes; n >= unit && exp+1 < len(prefixes); n /= unit {
		div *= unit
		exp++
	}
	// writing to the string.Builder can never return error
	_, _ = fmt.Fprintf(sb, "%6.1f %cB", float64(bytes)/float64(div), prefixes[exp])
}
