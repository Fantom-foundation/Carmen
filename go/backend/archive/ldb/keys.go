package ldb

import (
	"encoding/binary"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const blockSize = 8                 // block number size (uint64)
const maxBlock = 0xFFFFFFFFFFFFFFFF // max block number (uint64)
const reincSize = 4                 // reincarnation (uint32)

type blockKey [1 + blockSize]byte

func (k *blockKey) set(block uint64) {
	k[0] = byte(common.BlockArchiveKey)
	binary.LittleEndian.PutUint64(k[1:], maxBlock-block)
}

func (k *blockKey) get() (block uint64) {
	return maxBlock - binary.LittleEndian.Uint64(k[1:])
}

func getLastBlockRange() util.Range {
	var start, end blockKey
	start[0] = byte(common.BlockArchiveKey)
	end[0] = byte(common.BlockArchiveKey)
	for i := 1; i < 1+blockSize; i++ {
		end[i] = 0xFF
	}
	return util.Range{Start: start[:], Limit: end[:]}
}

type accountBlockKey [1 + common.AddressSize + blockSize]byte

func (k *accountBlockKey) set(table common.TableSpace, account common.Address, block uint64) {
	k[0] = byte(table)
	copy(k[1:1+common.AddressSize], account[:])
	binary.LittleEndian.PutUint64(k[1+common.AddressSize:], maxBlock-block)
}

func (k *accountBlockKey) getRange() util.Range {
	var end accountBlockKey
	copy(end[0:1+common.AddressSize], k[:])
	for i := 1 + common.AddressSize; i < 1+common.AddressSize+blockSize; i++ {
		end[i] = 0xFF
	}
	return util.Range{Start: k[:], Limit: end[:]}
}

type accountKeyBlockKey [1 + common.AddressSize + reincSize + common.KeySize + blockSize]byte

func (k *accountKeyBlockKey) set(table common.TableSpace, account common.Address, reincarnation int, slot common.Key, block uint64) {
	k[0] = byte(table)
	copy(k[1:1+common.AddressSize], account[:])
	binary.LittleEndian.PutUint32(k[1+common.AddressSize:], uint32(reincarnation))
	copy(k[1+common.AddressSize+reincSize:], slot[:])
	binary.LittleEndian.PutUint64(k[1+common.AddressSize+reincSize+common.KeySize:], maxBlock-block)
}

func (k *accountKeyBlockKey) getRange() util.Range {
	var end accountKeyBlockKey
	copy(end[0:1+common.AddressSize+reincSize+common.KeySize], k[:])
	for i := 1 + common.AddressSize + reincSize + common.KeySize; i < 1+common.AddressSize+reincSize+common.KeySize+blockSize; i++ {
		end[i] = 0xFF
	}
	return util.Range{Start: k[:], Limit: end[:]}
}

type accountStatusValue [1 + reincSize]byte

func (k *accountStatusValue) set(exists bool, reincarnation int) {
	if exists {
		k[0] = 1
	} else {
		k[0] = 0
	}
	binary.LittleEndian.PutUint32(k[1:], uint32(reincarnation))
}

func (k *accountStatusValue) get() (exists bool, reincarnation int) {
	exists = k[0] != 0
	reincarnation = int(binary.LittleEndian.Uint32(k[1:]))
	return exists, reincarnation
}
