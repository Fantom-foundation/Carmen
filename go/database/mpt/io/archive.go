// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package io

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"

	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"github.com/Fantom-foundation/Carmen/go/common/interrupt"
	"github.com/Fantom-foundation/Carmen/go/state"

	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
	"golang.org/x/exp/maps"
)

// This file provides a pair of import and export functions capable of
// serializing the content of an Archive into a single, payload-only data
// blob with build-in consistency check which can be utilized for safely
// transferring state information between systems.
//
// Format:
//
//  file   ::= <magic-number> <version> [<code>]* [<update>]*
//  code   ::= 'C' <2-byte big-endian code length> <code>
//  update ::= 'U' <4-byte big-endian block> [<hash>]+ [<change>]+
//  hash   ::= 'H' <1-byte hash type identifier> <state-hash>
//  change ::= 'A' <address>           // starts a new account scope
//           | 'R'                     // reset the current account
//           | 'B' <balance>           // update the current account's balance
//           | 'N' <nonce>             // update the current account's nonce
//           | 'c' <code-hash>         // update the current account's code
//           | 'V' <key> <value>       // update the value of a storage slot
//           | 'D' <key>               // delete a storage slot
//
// All properties belong to the account preceding it. The produced data stream
// may be further compressed (e.g. using Gzip) to reduce its size.

var archiveMagicNumber []byte = []byte("Fantom-Archive-State")

const archiveFormatVersion = byte(1)

func ExportArchive(ctx context.Context, directory string, out io.Writer) error {
	info, err := CheckMptDirectoryAndGetInfo(directory)
	if err != nil {
		return fmt.Errorf("error in input directory: %v", err)
	}

	if info.Config.Name != mpt.S5ArchiveConfig.Name {
		return fmt.Errorf("can only support export of S5 Archive instances, found %v in directory", info.Config.Name)
	}

	archive, err := mpt.OpenArchiveTrie(directory, info.Config, mpt.NodeCacheConfig{}, mpt.ArchiveConfig{})
	if err != nil {
		return err
	}

	// Start with the magic number.
	if _, err := out.Write(archiveMagicNumber); err != nil {
		return err
	}

	// Add a version number.
	if _, err := out.Write([]byte{archiveFormatVersion}); err != nil {
		return err
	}

	// Write out codes.
	codes := archive.GetCodes()
	if err = writeCodes(codes, out); err != nil {
		return err
	}

	// Write out updates.
	maxBlock, empty, err := archive.GetBlockHeight()
	if err != nil {
		return fmt.Errorf("failed to get max block height: %w", err)
	}
	if empty {
		return archive.Close()
	}

	// Encode diff of each individual block.
	for block := uint64(0); block <= maxBlock; block++ {
		if interrupt.IsCancelled(ctx) {
			return errors.Join(interrupt.ErrCanceled, archive.Close())
		}
		diff, err := archive.GetDiffForBlock(block)
		if err != nil {
			return fmt.Errorf("failed to get diff for block %d: %w", block, err)
		}
		if len(diff) == 0 {
			continue
		}

		// Encode block number.
		b := []byte{byte('U'), 0, 0, 0, 0}
		binary.BigEndian.PutUint32(b[1:], uint32(block))
		if _, err := out.Write(b); err != nil {
			return err
		}

		// Encode the block hash.
		hash, err := archive.GetHash(block)
		if err != nil {
			return err
		}
		if _, err := out.Write([]byte{byte('H'), byte(EthereumHash)}); err != nil {
			return err
		}
		if _, err := out.Write(hash[:]); err != nil {
			return err
		}

		// Encode changes of this block.
		addresses := maps.Keys(diff)
		sort.Slice(addresses, func(i, j int) bool { return bytes.Compare(addresses[i][:], addresses[j][:]) < 0 })
		for _, address := range addresses {
			if _, err := out.Write([]byte{'A'}); err != nil {
				return err
			}
			if _, err := out.Write(address[:]); err != nil {
				return err
			}
			accountDiff := diff[address]
			if accountDiff.Reset {
				if _, err := out.Write([]byte{'R'}); err != nil {
					return err
				}
			}
			if accountDiff.Balance != nil {
				if _, err := out.Write([]byte{'B'}); err != nil {
					return err
				}
				b := accountDiff.Balance.Bytes32()
				if _, err := out.Write(b[:]); err != nil {
					return err
				}
			}
			if accountDiff.Nonce != nil {
				if _, err := out.Write([]byte{'N'}); err != nil {
					return err
				}
				if _, err := out.Write((*accountDiff.Nonce)[:]); err != nil {
					return err
				}
			}
			if accountDiff.Code != nil {
				if _, err := out.Write([]byte{'c'}); err != nil {
					return err
				}
				if _, err := out.Write((*accountDiff.Code)[:]); err != nil {
					return err
				}
			}
			keys := maps.Keys(accountDiff.Storage)
			sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
			for _, key := range keys {
				value := accountDiff.Storage[key]
				if (value == common.Value{}) {
					if _, err := out.Write([]byte{'D'}); err != nil {
						return err
					}
					if _, err := out.Write(key[:]); err != nil {
						return err
					}
				} else {
					if _, err := out.Write([]byte{'V'}); err != nil {
						return err
					}
					if _, err := out.Write(key[:]); err != nil {
						return err
					}
					if _, err := out.Write(value[:]); err != nil {
						return err
					}
				}
			}
		}
	}

	return archive.Close()
}

func ImportArchive(directory string, in io.Reader) error {
	// check that the destination directory is an empty directory
	if err := checkEmptyDirectory(directory); err != nil {
		return err
	}
	liveDbDir := path.Join(directory, "tmp-live-db")
	return errors.Join(
		importArchive(liveDbDir, directory, in),
		os.RemoveAll(liveDbDir), // live db is deleted at the end
	)
}

func ImportLiveAndArchive(directory string, in io.Reader) error {
	// check that the destination directory is an empty directory
	if err := checkEmptyDirectory(directory); err != nil {
		return err
	}
	liveDbDir := path.Join(directory, "live")
	archiveDbDir := path.Join(directory, "archive")
	return importArchive(liveDbDir, archiveDbDir, in)
}

func importArchive(liveDbDir, archiveDbDir string, in io.Reader) (err error) {
	// Start by checking the magic number.
	buffer := make([]byte, len(archiveMagicNumber))
	if _, err := io.ReadFull(in, buffer); err != nil {
		return err
	} else if !bytes.Equal(buffer, archiveMagicNumber) {
		// Specify error if incorrect genesis is passed
		if bytes.Contains(buffer, stateMagicNumber) {
			return fmt.Errorf("incorrect genesis+command combination\n your genesis is meant to be used with import-live")
		}
		return errors.New("invalid format, unknown magic number")
	}

	// Check the version number.
	if _, err := io.ReadFull(in, buffer[0:1]); err != nil {
		return err
	} else if buffer[0] != archiveFormatVersion {
		return fmt.Errorf("invalid format, unsupported version")
	}

	// Create a live-DB updated in parallel for faster hash computation.
	live, err := mpt.OpenGoFileState(liveDbDir, mpt.S5LiveConfig, mpt.NodeCacheConfig{})
	if err != nil {
		return fmt.Errorf("failed to create auxiliary live DB: %w", err)
	}
	defer func() {
		err = errors.Join(
			err,
			live.Close(),
		)
	}()

	// Create an empty archive.
	archive, err := mpt.OpenArchiveTrie(archiveDbDir, mpt.S5ArchiveConfig, mpt.NodeCacheConfig{}, mpt.ArchiveConfig{})
	if err != nil {
		return fmt.Errorf("failed to create empty state: %w", err)
	}
	defer func() {
		err = errors.Join(err, archive.Close())
	}()

	// Restore the archive from the input file.
	context := newImportContext()
	for {
		// Read prefix determining the next input marker.
		if _, err := io.ReadFull(in, buffer[0:1]); err != nil {
			if err == io.EOF {
				return context.finishCurrentBlock(archive, live)
			}
			return err
		}
		switch buffer[0] {
		case 'A':
			address := common.Address{}
			if _, err := io.ReadFull(in, address[:]); err != nil {
				return err
			}
			context.setAccount(address)
		case 'C':
			code, err := readCode(in)
			if err != nil {
				return err
			}
			context.addCode(code)
		case 'U':
			if err := context.finishCurrentBlock(archive, live); err != nil {
				return err
			}
			if _, err := io.ReadFull(in, buffer[0:4]); err != nil {
				return err
			}
			context.setBlock(uint64(binary.BigEndian.Uint32(buffer)))

		case 'H':
			if _, err := io.ReadFull(in, buffer[0:1]); err != nil {
				return err
			}
			hashType := HashType(buffer[0])
			hash := common.Hash{}
			if _, err := io.ReadFull(in, hash[:]); err != nil {
				return err
			}
			if hashType == EthereumHash {
				context.setBlockHash(hash)
			}

		case 'R':
			context.deleteAccount()

		case 'B':
			var balance [amount.BytesLength]byte
			if _, err := io.ReadFull(in, balance[:]); err != nil {
				return err
			}
			context.setBalance(amount.NewFromBytes(balance[:]...))

		case 'N':
			nonce := common.Nonce{}
			if _, err := io.ReadFull(in, nonce[:]); err != nil {
				return err
			}
			context.setNonce(nonce)

		case 'c':
			hash := common.Hash{}
			if _, err := io.ReadFull(in, hash[:]); err != nil {
				return err
			}
			if err := context.setCode(hash); err != nil {
				return err
			}

		case 'V':
			key := common.Key{}
			value := common.Value{}
			if _, err := io.ReadFull(in, key[:]); err != nil {
				return err
			}
			if _, err := io.ReadFull(in, value[:]); err != nil {
				return err
			}
			context.setSlot(key, value)

		case 'D':
			key := common.Key{}
			if _, err := io.ReadFull(in, key[:]); err != nil {
				return err
			}
			context.deleteSlot(key)

		default:
			return fmt.Errorf("format error encountered, unexpected token type: %c", buffer[0])
		}
	}
}

type importContext struct {
	codes                 map[common.Hash][]byte
	currentAccount        common.Address
	currentBlock          uint64
	currentBlockHash      common.Hash
	currentBlockHashFound bool
	currentUpdate         common.Update
}

func newImportContext() *importContext {
	return &importContext{
		codes: map[common.Hash][]byte{
			common.Keccak256([]byte{}): {},
		},
	}
}

func (c *importContext) addCode(code []byte) {
	c.codes[common.Keccak256(code)] = code
}

func (c *importContext) setBlock(block uint64) {
	c.currentBlock = block
}

func (c *importContext) setBlockHash(hash common.Hash) {
	c.currentBlockHash = hash
	c.currentBlockHashFound = true
}

func (c *importContext) setAccount(address common.Address) {
	c.currentAccount = address
}

func (c *importContext) deleteAccount() {
	c.currentUpdate.AppendDeleteAccount(c.currentAccount)
}

func (c *importContext) setBalance(balance amount.Amount) {
	c.currentUpdate.AppendBalanceUpdate(c.currentAccount, balance)
}

func (c *importContext) setNonce(nonce common.Nonce) {
	c.currentUpdate.AppendNonceUpdate(c.currentAccount, nonce)
}

func (c *importContext) setCode(hash common.Hash) error {
	code, found := c.codes[hash]
	if !found {
		return fmt.Errorf("missing code for hash %v in input file", hash)
	}
	c.currentUpdate.AppendCodeUpdate(c.currentAccount, code)
	return nil
}

func (c *importContext) setSlot(key common.Key, value common.Value) {
	c.currentUpdate.AppendSlotUpdate(c.currentAccount, key, value)
}

func (c *importContext) deleteSlot(key common.Key) {
	c.currentUpdate.AppendSlotUpdate(c.currentAccount, key, common.Value{})
}

func (c *importContext) finishCurrentBlock(archive archive.Archive, live state.LiveDB) error {
	if c.currentUpdate.IsEmpty() {
		return nil
	}
	if !c.currentBlockHashFound {
		return fmt.Errorf("input format error: no hash for block %d", c.currentBlock)
	}
	hints, err := live.Apply(c.currentBlock, c.currentUpdate)
	if err != nil {
		return err
	}
	if err := archive.Add(c.currentBlock, c.currentUpdate, hints); err != nil {
		return err
	}
	hints.Release()
	hash, err := archive.GetHash(c.currentBlock)
	if err != nil {
		return err
	}
	if hash != c.currentBlockHash {
		return fmt.Errorf("invalid hash for block %d: from input %x, restored hash %x", c.currentBlock, c.currentBlockHash, hash)
	}

	c.currentUpdate = common.Update{}
	c.currentBlockHashFound = false
	return nil
}
