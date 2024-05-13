// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package mpt

// MptConfig defines a set of configuration options for customizing the MPT
// implementation. It is mainly intended to facilitate the accurate modeling
// of Ethereum's MPT implementation (see schema 5) but may also be used for
// experimenting with design options.
type MptConfig struct {
	// A descriptive name for this configuration. It has no effect except for
	// logging and debugging purposes.
	Name string

	// If set to true, the address of accounts and keys of values are hashed
	// using keccak256 before being used to navigate the trie. If false, the
	// addresses and keys are directly used as paths for the MPT.
	UseHashedPaths bool

	// If enabled, leaf nodes are tracking the number of nibbles of their path
	// not covered by parent nodes. If disabled, this information is not
	// maintained. In either way, the full path is stored in leaf nodes.
	// The suffix length is required for Ethereum's MPT variant.
	TrackSuffixLengthsInLeafNodes bool

	// The hashing algorithm to be used in the MPT implementation.
	Hashing hashAlgorithm

	// Determines whether hashes are stored with nodes or with the parents.
	HashStorageLocation HashStorageLocation
}

var S4LiveConfig = MptConfig{
	Name:                          "S4-Live",
	UseHashedPaths:                false,
	TrackSuffixLengthsInLeafNodes: false,
	Hashing:                       DirectHashing,
	HashStorageLocation:           HashStoredWithParent,
}

var S4ArchiveConfig = MptConfig{
	Name:                          "S4-Archive",
	UseHashedPaths:                false,
	TrackSuffixLengthsInLeafNodes: false,
	Hashing:                       DirectHashing,
	HashStorageLocation:           HashStoredWithNode,
}

var S5LiveConfig = MptConfig{
	Name:                          "S5-Live",
	UseHashedPaths:                true,
	TrackSuffixLengthsInLeafNodes: true,
	Hashing:                       EthereumLikeHashing, // requires tracking of suffix lengths
	HashStorageLocation:           HashStoredWithParent,
}

var S5ArchiveConfig = MptConfig{
	Name:                          "S5-Archive",
	UseHashedPaths:                true,
	TrackSuffixLengthsInLeafNodes: true,
	Hashing:                       EthereumLikeHashing, // requires tracking of suffix lengths
	HashStorageLocation:           HashStoredWithNode,
}

var allMptConfigs = []MptConfig{
	S4LiveConfig, S4ArchiveConfig,
	S5LiveConfig, S5ArchiveConfig,
}

// GetConfigByName attempts to locate a configuration with the given name.
func GetConfigByName(name string) (MptConfig, bool) {
	for _, config := range allMptConfigs {
		if config.Name == name {
			return config, true
		}
	}
	return MptConfig{}, false
}

type HashStorageLocation bool

const (
	// HashStoredWithNode is a configuration option where the hash of a node
	// is stored together with the node. Storing hashes with nodes avoids
	// retaining multiple copies of the same hash in Archives. However, it
	// increases the number of disk accesses required for computing hashes.
	// It is thus the recommended mode for Archives
	HashStoredWithNode HashStorageLocation = true
	// HashStoredWithParent is a configuration option where hashes of nodes
	// are stored in their respective parent nodes. For trees, this mode is
	// equally disk-space efficient as storing hashes in nodes, but less
	// disk-seek operations are required for re-computing hashes. It is thus
	// the main mode recommended for LiveDB configurations. For Archives
	// this mode results in the redundant storage of hashes, since each
	// node may have multiple parent nodes.
	HashStoredWithParent HashStorageLocation = false
)

func (l HashStorageLocation) String() string {
	switch l {
	case HashStoredWithNode:
		return "HashStoredWithNode"
	case HashStoredWithParent:
		return "HashStoredWithParent"
	default:
		return "?"
	}
}
