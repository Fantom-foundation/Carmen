package common

import (
	"fmt"
	"strings"
)

// MemoryFootprint describes the memory consumption of a database structure
type MemoryFootprint struct {
	value    uintptr
	children map[string]*MemoryFootprint
}

// NewMemoryFootprint creates a new MemoryFootprint instance for a database structure
func NewMemoryFootprint(value uintptr) *MemoryFootprint {
	return &MemoryFootprint{
		value:    value,
		children: make(map[string]*MemoryFootprint),
	}
}

// AddChild allows to attach a MemoryFootprint of the database structure subcomponent
func (mf *MemoryFootprint) AddChild(name string, child *MemoryFootprint) {
	mf.children[name] = child
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
func (mf *MemoryFootprint) ToString(name string) (str string, err error) {
	var sb strings.Builder
	err = mf.toStringBuilder(&sb, name)
	return sb.String(), err
}

func (mf *MemoryFootprint) toStringBuilder(sb *strings.Builder, path string) (err error) {
	err = memoryAmountToString(sb, mf.Total())
	if err != nil {
		return
	}
	sb.WriteRune(' ')
	sb.WriteString(path)
	sb.WriteRune('\n')
	for name, footprint := range mf.children {
		fullPath := path + "/" + name
		err = footprint.toStringBuilder(sb, fullPath)
		if err != nil {
			return
		}
	}
	return
}

func memoryAmountToString(sb *strings.Builder, bytes uintptr) (err error) {
	const unit = 1024
	const prefixes = "KMGTPE"
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit && exp+1 < len(prefixes); n /= unit {
		div *= unit
		exp++
	}
	_, err = fmt.Fprintf(sb, "%.1f %cB", float64(bytes)/float64(div), prefixes[exp])
	return err
}
