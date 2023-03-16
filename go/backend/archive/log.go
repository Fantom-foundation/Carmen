package archive

import "github.com/Fantom-foundation/Carmen/go/common"

type Topic = common.Hash

type Log struct {
	// Payload
	Address common.Address
	Topics  [5]Topic
	Data    []byte

	// Metadata
	Block uint64
	// TODO: add other log meta data information
}

type LogFilter struct {
	From, To  uint64           // the relevant block range
	Addresses []common.Address // one-of those addresses, empty or nil = any address
	Topics    [5][]Topic       // filter for topics, empty or nil = any topic
}

func (f *LogFilter) Match(log *Log) bool {
	// Check the block and address.
	if !(f.From <= log.Block && log.Block <= f.To) {
		return false
	}

	// Check the address (if constraint).
	if len(f.Addresses) > 0 {
		found := false
		for i := 0; !found && i < len(f.Addresses); i++ {
			found = f.Addresses[i] == log.Address
		}
		if !found {
			return false
		}
	}

	// Check the topic pattern.
	for i := 0; i < 5; i++ {
		if f.Topics[i] != nil {
			found := false
			for j := 0; !found && j < len(f.Topics[i]); j++ {
				found = f.Topics[i][j] == log.Topics[i]
			}
			if !found {
				return false
			}
		}
	}
	return true
}

type LogArchive interface {
	Add(block uint64, logs []*Log) error
	Get(filter *LogFilter) ([]*Log, error)
	GetHash(block uint64) common.Hash
	Verify(block uint64) bool
}
