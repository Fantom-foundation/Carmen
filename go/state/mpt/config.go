package mpt

// MptConfig defines a set of configuration options for customizing the MPT
// implementation. It is mainly intended to facilitate the accurate modeling
// of Ethereums MPT implementation (see schema 5) but may also be used for
// experimenting with design options.
type MptConfig struct {
	// A describtive name for this configuration. It has no effect except for
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

	// The number of nodes to be retained in the node cache. If set to 0, caching is disabled.
	NodeCacheSize int

	// The number of node hashes to be retained in an in-memory cache cache. If set to 0, caching is disabled.
	HashCacheSize int

	// The size of the cache used for hashing addresses (only useful if paths are hashed).
	AddressHashCacheSize int

	// The size of the cache used for hashing keys (only useful if paths are hashed).
	KeyHashCacheSize int
}

var S4Config = MptConfig{
	Name:                          "S4",
	UseHashedPaths:                false,
	TrackSuffixLengthsInLeafNodes: false,
	Hashing:                       DirectHashing,
	NodeCacheSize:                 10_000_000,
	HashCacheSize:                 10_000_000,
}

var S5Config = MptConfig{
	Name:                          "S5",
	UseHashedPaths:                true,
	TrackSuffixLengthsInLeafNodes: true,
	Hashing:                       EthereumLikeHashing, // requires tracking of suffix lengths
	NodeCacheSize:                 10_000_000,
	HashCacheSize:                 10_000_000,
	AddressHashCacheSize:          100_000,
	KeyHashCacheSize:              100_000,
}

var allMptConfigs = []MptConfig{S4Config, S5Config}
