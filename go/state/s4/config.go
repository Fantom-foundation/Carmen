package s4

// MptConfig defines a set of configuration options for customizing the MPT
// implementation. It is mainly intended to facilitate the accurate modeling
// of Ethereums MPT implementation (see schema 5) but may also be used for
// experimenting with design options.
type MptConfig struct {
	// A describtive name for this configuration. It has no effect except for
	// logging and debugging purposes.
	Name string

	// If set to true, the address of accounts is hashed using keccak256. If
	// false, the address is directly used as key for the MPT.
	UseHashedAccountAddress bool

	// If enabled, leafs retain partial paths, while if disabled, the full path
	// is stored. The partial path mode is required for Ethereum's MPT variant.
	// While in many cases being a few bytes more compact than the full path, in
	// the worst case, the need for storing the length can result in higher disk
	// usage. For fixed-size storage, the worst case has to be assumed, causing
	// disk requirements for paths
	PartialPathsInLeafs bool

	// The hashing algorithm to be used in the MPT implementation.
	Hasher Hasher
}

var S4Config = MptConfig{
	Name:                    "S4",
	UseHashedAccountAddress: false,
	PartialPathsInLeafs:     false,
	Hasher:                  DirectHasher{},
}

var S5Config = MptConfig{
	Name:                    "S5",
	UseHashedAccountAddress: true,
	PartialPathsInLeafs:     true,
	Hasher:                  MptHasher{},
}

var allMptConfigs = []MptConfig{S4Config, S5Config}
