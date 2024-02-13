package ldb

import (
	"encoding/binary"
	"github.com/Fantom-foundation/Carmen/go/backend"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const blockSize = 8                 // block number size (uint64)
const maxBlock = 0xFFFFFFFFFFFFFFFE // max block number (uint64) - must be less than the max value to fit into limit range
const reincSize = 4                 // reincarnation (uint32)

var limitBlock = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF} // max range value, must be greater than maxBlock

// blockKey is a key for block table, it consists of
// * the tablespace
// * the block number, represented as an inverse value to sort from the highest block
type blockKey [1 + blockSize]byte

func (k *blockKey) set(block uint64) {
	k[0] = byte(backend.BlockArchiveKey)
	binary.BigEndian.PutUint64(k[1:], maxBlock-block)
}

func (k *blockKey) get() (block uint64) {
	return maxBlock - binary.BigEndian.Uint64(k[1:])
}

// getBlockKeyRangeFrom provides a key range for iterating blocks from the given block to the first block
func getBlockKeyRangeFrom(block uint64) util.Range {
	var start, end blockKey
	start.set(block)
	end[0] = start[0]
	copy(end[1:], limitBlock)
	return util.Range{Start: start[:], Limit: end[:]}
}

// getBlockKeyRangeFromHighest provides a key range for iterating from the highest block to the first
func getBlockKeyRangeFromHighest() util.Range {
	return getBlockKeyRangeFrom(maxBlock)
}

// accountBlockKey is a key for account details tables, it consists of
// * the tablespace
// * the account address
// * the block number
type accountBlockKey [1 + common.AddressSize + blockSize]byte

func (k *accountBlockKey) set(table backend.TableSpace, account common.Address, block uint64) {
	k[0] = byte(table)
	copy(k[1:1+common.AddressSize], account[:])
	binary.BigEndian.PutUint64(k[1+common.AddressSize:], maxBlock-block)
}

// getRange provides a key range for iterating the account value from the given block to the first block
func (k *accountBlockKey) getRange() util.Range {
	end := *k
	copy(end[1+common.AddressSize:], limitBlock)
	return util.Range{Start: k[:], Limit: end[:]}
}

// accountKeyBlockKey is a key for storage slots, it consists of
// * the tablespace
// * the account address
// * the account reincarnation (incrementing a reincarnation invalidates the account storage)
// * the storage slot key
// * the block number
type accountKeyBlockKey [1 + common.AddressSize + reincSize + common.KeySize + blockSize]byte

func (k *accountKeyBlockKey) set(table backend.TableSpace, account common.Address, reincarnation int, slot common.Key, block uint64) {
	k[0] = byte(table)
	copy(k[1:1+common.AddressSize], account[:])
	binary.BigEndian.PutUint32(k[1+common.AddressSize:], uint32(reincarnation))
	copy(k[1+common.AddressSize+reincSize:], slot[:])
	binary.BigEndian.PutUint64(k[1+common.AddressSize+reincSize+common.KeySize:], maxBlock-block)
}

// getRange provides a key range for iterating the slot value from the given block to the first block
func (k *accountKeyBlockKey) getRange() util.Range {
	end := *k
	copy(end[1+common.AddressSize+reincSize+common.KeySize:], limitBlock)
	return util.Range{Start: k[:], Limit: end[:]}
}

// accountStatusValue is a value for account status, it consists of
// * the account existence status (1 for existing account, 0 otherwise)
// * the reincarnation number (references the storage, incremented on account creation/destroying)
type accountStatusValue [1 + reincSize]byte

func (k *accountStatusValue) set(exists bool, reincarnation int) {
	if exists {
		k[0] = 1
	} else {
		k[0] = 0
	}
	binary.BigEndian.PutUint32(k[1:], uint32(reincarnation))
}

func (k *accountStatusValue) get() (exists bool, reincarnation int) {
	exists = k[0] != 0
	reincarnation = int(binary.BigEndian.Uint32(k[1:]))
	return exists, reincarnation
}
