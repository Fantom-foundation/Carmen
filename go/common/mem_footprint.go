package common

import (
	"fmt"
	"strings"
)

type MemoryFootprint struct {
	value    uintptr
	children map[string]MemoryFootprint
}

func NewMemoryFootprint(value uintptr) MemoryFootprint {
	return MemoryFootprint{
		value:    value,
		children: make(map[string]MemoryFootprint),
	}
}

func (mf *MemoryFootprint) AddChild(name string, child MemoryFootprint) {
	mf.children[name] = child
}

func (mf *MemoryFootprint) Total() uintptr {
	total := mf.value
	for _, child := range mf.children {
		total += child.Total()
	}
	return total
}

func (mf *MemoryFootprint) ToString() (str string, err error) {
	var sb strings.Builder
	err = mf.toStringBuilder(&sb, "state")
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
