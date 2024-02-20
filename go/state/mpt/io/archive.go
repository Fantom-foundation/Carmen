package io

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/mpt"
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

func ExportArchive(directory string, out io.Writer) error {

	info, err := CheckMptDirectoryAndGetInfo(directory)
	if err != nil {
		return fmt.Errorf("error in input directory: %v", err)
	}

	if info.Config.Name != mpt.S5ArchiveConfig.Name {
		return fmt.Errorf("can only support export of S5 Archive instances, found %v in directory", info.Config.Name)
	}

	archive, err := mpt.OpenArchiveTrie(directory, info.Config, mpt.DefaultMptStateCapacity)
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
	codes, err := archive.GetCodes()
	if err != nil {
		return fmt.Errorf("failed to retrieve codes: %v", err)
	}
	if err := writeCodes(codes, out); err != nil {
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
				if _, err := out.Write((*accountDiff.Balance)[:]); err != nil {
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

	// Start by checking the magic number.
	buffer := make([]byte, len(archiveMagicNumber))
	if _, err := io.ReadFull(in, buffer); err != nil {
		return err
	} else if !bytes.Equal(buffer, archiveMagicNumber) {
		return fmt.Errorf("invalid format, wrong magic number")
	}

	// Check the version number.
	if _, err := io.ReadFull(in, buffer[0:1]); err != nil {
		return err
	} else if buffer[0] != archiveFormatVersion {
		return fmt.Errorf("invalid format, unsupported version")
	}

	// Create an empty archive.
	archive, err := mpt.OpenArchiveTrie(directory, mpt.S5ArchiveConfig, mpt.DefaultMptStateCapacity)
	if err != nil {
		return fmt.Errorf("failed to create empty state: %v", err)
	}
	defer func() {
		err = errors.Join(err, archive.Close())
	}()

	// Restore the archive from the input file.
	codes := map[common.Hash][]byte{
		common.Keccak256([]byte{}): {},
	}

	currentBlock := uint64(0)
	currentBlockHash := common.Hash{}
	currentBlockHashFound := false
	currentUpdate := common.Update{}

	finishCurrentBlock := func() error {
		if currentUpdate.IsEmpty() {
			return nil
		}
		if !currentBlockHashFound {
			return fmt.Errorf("input format error: no hash for block %d", currentBlock)
		}
		if err := archive.Add(currentBlock, currentUpdate, nil); err != nil {
			return err
		}
		hash, err := archive.GetHash(currentBlock)
		if err != nil {
			return err
		}
		if hash != currentBlockHash {
			return fmt.Errorf("invalid hash for block %d: from input %x, restored hash %x", currentBlock, currentBlockHash, hash)
		}

		currentUpdate = common.Update{}
		currentBlockHashFound = false
		return nil
	}

	currentAccount := common.Address{}
	for {
		// Read prefix determining the next input marker.
		if _, err := io.ReadFull(in, buffer[0:1]); err != nil {
			if err == io.EOF {
				return finishCurrentBlock()
			}
			return err
		}
		switch buffer[0] {
		case 'A':
			if _, err := io.ReadFull(in, currentAccount[:]); err != nil {
				return err
			}
		case 'C':
			code, err := readCode(in)
			if err != nil {
				return err
			}
			codes[common.Keccak256(code)] = code
		case 'U':
			if err := finishCurrentBlock(); err != nil {
				return err
			}
			if _, err := io.ReadFull(in, buffer[0:4]); err != nil {
				return err
			}
			currentBlock = uint64(binary.BigEndian.Uint32(buffer))

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
				currentBlockHash = hash
				currentBlockHashFound = true
			}

		case 'R':
			currentUpdate.DeletedAccounts = append(currentUpdate.DeletedAccounts, currentAccount)

		case 'B':
			balance := common.Balance{}
			if _, err := io.ReadFull(in, balance[:]); err != nil {
				return err
			}
			currentUpdate.Balances = append(currentUpdate.Balances, common.BalanceUpdate{
				Account: currentAccount,
				Balance: balance,
			})

		case 'N':
			nonce := common.Nonce{}
			if _, err := io.ReadFull(in, nonce[:]); err != nil {
				return err
			}
			currentUpdate.Nonces = append(currentUpdate.Nonces, common.NonceUpdate{
				Account: currentAccount,
				Nonce:   nonce,
			})

		case 'c':
			hash := common.Hash{}
			if _, err := io.ReadFull(in, hash[:]); err != nil {
				return err
			}
			code, found := codes[hash]
			if !found {
				return fmt.Errorf("missing code for hash %v in input file", hash)
			}
			currentUpdate.Codes = append(currentUpdate.Codes, common.CodeUpdate{
				Account: currentAccount,
				Code:    code,
			})

		case 'V':
			key := common.Key{}
			value := common.Value{}
			if _, err := io.ReadFull(in, key[:]); err != nil {
				return err
			}
			if _, err := io.ReadFull(in, value[:]); err != nil {
				return err
			}
			currentUpdate.Slots = append(currentUpdate.Slots, common.SlotUpdate{
				Account: currentAccount,
				Key:     key,
				Value:   value,
			})

		case 'D':
			key := common.Key{}
			if _, err := io.ReadFull(in, key[:]); err != nil {
				return err
			}
			currentUpdate.Slots = append(currentUpdate.Slots, common.SlotUpdate{
				Account: currentAccount,
				Key:     key,
			})

		default:
			return fmt.Errorf("format error encountered, unexpected token type: %c", buffer[0])
		}
	}
}
