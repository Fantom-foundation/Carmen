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

import (
	"fmt"
	"strconv"
)

// Property is an optional parameter for configuring a DB instance.
type Property string

const (
	// LiveDBCache is a configuration property defining an approximate upper
	// limit for the in-memory node-cache size of the LiveDB in bytes.
	LiveDBCache = Property("LiveDbCache")
	// ArchiveCache is a configuration property defining an approximate upper
	// limit for the in-memory node-cache size of the Archive in bytes.
	ArchiveCache = Property("ArchiveCache")
	// StorageCache is the size of cache used for account storages.
	// This cache is utilized everytime a storage slot of an account is accessed.
	// Before expanding expensive data structures, it is checked if the storage slot
	// is available in this cache, and obtained there if it exists.
	// If the storage slot is updated, it is updated in this cache first,
	// before being flushed into underlying structures later.
	StorageCache = Property("StorageCache")

	// CheckpointInterval/Period determines how often an Archive creates checkpoints.
	// If either of the periods is passed (time or block number) a checkpoint is created.
	// Checkpoints are necessary for resetting corrupted databases.

	// CheckpointInterval determines how often (in blocks) an Archive creates checkpoints.
	CheckpointInterval = Property("CheckpointInterval")
	// CheckpointPeriod determines how often (in minutes) an Archive creates checkpoints.
	CheckpointPeriod = Property("CheckpointPeriod")
)

// Properties are optional settings which may influence the
// behavior of a Database, but do not alter compatibility.
// Configurations referencing the same Variant, Schema, and
// Archive are expected to be compatible. Example properties
// are configuration parameters for internal caches.
type Properties map[Property]string

// GetInteger is a utility function for Properties to retrieve numeric values.
func (p *Properties) GetInteger(name Property, fallback int) (int, error) {
	if value, found := (*p)[name]; found {
		res, err := strconv.Atoi(value)
		if err != nil {
			return 0, fmt.Errorf("invalid value for '%s' property: %v", name, value)
		}
		return res, nil
	}
	return fallback, nil
}

// SetInteger is a utility function for Properties to set numeric values.
func (p *Properties) SetInteger(name Property, value int) {
	if *p == nil {
		*p = map[Property]string{}
	}
	(*p)[name] = strconv.Itoa(value)
}
