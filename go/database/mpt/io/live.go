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
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/amount"
	"github.com/Fantom-foundation/Carmen/go/common/genesis"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
)

// This file provides a pair of import and export functions capable of
// serializing the content of a LiveDB into a single, payload-only data
// blob with build-in consistency check which can be utilized for safely
// transferring state information between systems.
//
// Format:
//
//  file  ::= <magic-number> <version> [<hash>]+ [<code>]* [<entry>]*
//  hash  ::= 'H' <1-byte hash type identifier> <state-hash>
//  code  ::= 'C' <2-byte big-endian code length> <code>
//  entry ::= 'A' <address> <balance> <nonce> <code-hash>
//          | 'S' <key> <value>
//
// All values belong to the account preceding it. The produced data stream
// may be further compressed (e.g. using Gzip) to reduce its size.

// Export opens a LiveDB instance retained in the given directory and writes
// its content to the given output writer. The result contains all the
// information required by the Import function below to reconstruct the full
// state of the LiveDB.
func Export(ctx context.Context, directory string, out io.Writer) error {
	info, err := CheckMptDirectoryAndGetInfo(directory)
	if err != nil {
		return fmt.Errorf("error in input directory: %v", err)
	}

	if info.Config.Name != mpt.S5LiveConfig.Name {
		return fmt.Errorf("can only support export of LiveDB instances, found %v in directory", info.Mode)
	}

	db, err := mpt.OpenGoFileState(directory, info.Config, mpt.NodeCacheConfig{})
	if err != nil {
		return fmt.Errorf("failed to open LiveDB: %v", err)
	}
	defer db.Close()

	_, err = mpt.ExportLive(ctx, db, out)
	return err
}

// ExportBlockFromArchive exports LiveDB genesis for a single given block from an Archive.
// Note: block must be <= of Archive block height.
func ExportBlockFromArchive(ctx context.Context, directory string, out io.Writer, block uint64) error {
	info, err := CheckMptDirectoryAndGetInfo(directory)
	if err != nil {
		return fmt.Errorf("error in input directory: %v", err)
	}

	if info.Config.Name != mpt.S5ArchiveConfig.Name {
		return fmt.Errorf("can only support export of S5 Archive instances, found %v in directory", info.Config.Name)
	}

	archive, err := mpt.OpenArchiveTrie(directory, info.Config, mpt.NodeCacheConfig{})
	if err != nil {
		return err
	}

	defer archive.Close()
	_, err = mpt.ExportLive(ctx, mpt.NewExportableArchiveTrie(archive, block), out)
	return err
}

// ImportLiveDb creates a fresh StateDB in the given directory and fills it
// with the content read from the given reader.
func ImportLiveDb(directory string, in io.Reader) error {
	_, _, err := runImport(directory, in, mpt.S5LiveConfig)
	return err
}

// InitializeArchive creates a fresh Archive in the given directory containing
// the state read from the input stream at the given block. All states before
// the given block are empty.
func InitializeArchive(directory string, in io.Reader, block uint64) (err error) {
	// The import creates a live-DB state that initializes the Archive.
	root, hash, err := runImport(directory, in, mpt.S5ArchiveConfig)
	if err != nil {
		return err
	}

	// Seal the data by marking the content as immutable.
	forestFile := directory + string(os.PathSeparator) + "forest.json"
	metaData, err := os.ReadFile(forestFile)
	if err != nil {
		return err
	}
	metaData = []byte(strings.Replace(string(metaData), "\"Mutable\":true", "\"Mutable\":false", 1))
	if err := os.WriteFile(forestFile, metaData, 0600); err != nil {
		return err
	}

	// Create a root file listing block roots.
	roots := make([]mpt.Root, block+1)
	for i := uint64(0); i < block; i++ {
		roots[i] = mpt.Root{
			NodeRef: mpt.NewNodeReference(mpt.EmptyId()),
			Hash:    mpt.EmptyNodeEthereumHash,
		}
	}
	roots[block] = mpt.Root{
		NodeRef: mpt.NewNodeReference(root),
		Hash:    hash,
	}
	if err := mpt.StoreRoots(directory+string(os.PathSeparator)+"roots.dat", roots); err != nil {
		return err
	}
	return nil
}

func runImport(directory string, in io.Reader, config mpt.MptConfig) (root mpt.NodeId, hash common.Hash, err error) {
	// check that the destination directory is an empty directory
	if err := checkEmptyDirectory(directory); err != nil {
		return root, hash, err
	}

	// Start by checking the magic number.
	buffer := make([]byte, len(genesis.StateMagicNumber))
	if _, err := io.ReadFull(in, buffer); err != nil {
		return root, hash, err
	} else if !bytes.Equal(buffer, genesis.StateMagicNumber) {
		return root, hash, fmt.Errorf("invalid format, wrong magic number")
	}

	// Check the version number.
	if _, err := io.ReadFull(in, buffer[0:1]); err != nil {
		return root, hash, err
	} else if buffer[0] != genesis.FormatVersion {
		return root, hash, fmt.Errorf("invalid format, unsupported version")
	}

	// Create a state.
	db, err := mpt.OpenGoFileState(directory, config, mpt.NodeCacheConfig{})
	if err != nil {
		return root, hash, fmt.Errorf("failed to create empty state: %v", err)
	}
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	var (
		addr    common.Address
		balance [amount.BytesLength]byte
		key     common.Key
		value   common.Value
		nonce   common.Nonce
	)

	// Read the rest and build the state.
	buffer = buffer[0:1]
	codes := map[common.Hash][]byte{
		common.Keccak256([]byte{}): {},
	}

	counter := 0

	hashFound := false
	var stateHash common.Hash
	for {
		// Update hashes periodically to avoid running out of memory
		// for nodes with dirty hashes.
		counter++
		if (counter % 100_000) == 0 {
			if _, err := db.GetHash(); err != nil {
				return root, hash, fmt.Errorf("failed to update hashes: %v", err)
			}
		}

		if _, err := io.ReadFull(in, buffer); err != nil {
			if err == io.EOF {
				if !hashFound {
					return root, hash, fmt.Errorf("file does not contain a compatible state hash")
				}
				// Check the final hash.
				hash, err := db.GetHash()
				if err != nil {
					return root, hash, err
				}
				if stateHash != hash {
					return root, hash, fmt.Errorf("failed to reproduce valid state, hashes do not match")
				}
				return db.GetRootId(), hash, nil
			}
			return root, hash, err
		}
		switch buffer[0] {
		case 'A':
			if _, err := io.ReadFull(in, addr[:]); err != nil {
				return root, hash, err
			}
			if _, err := io.ReadFull(in, balance[:]); err != nil {
				return root, hash, err
			}
			if err := db.SetBalance(addr, amount.NewFromBytes(balance[:]...)); err != nil {
				return root, hash, err
			}
			if _, err := io.ReadFull(in, nonce[:]); err != nil {
				return root, hash, err
			}
			if err := db.SetNonce(addr, nonce); err != nil {
				return root, hash, err
			}
			if _, err := io.ReadFull(in, hash[:]); err != nil {
				return root, hash, err
			}
			if code, found := codes[hash]; found {
				if err := db.SetCode(addr, code); err != nil {
					return root, hash, err
				}
			} else {
				return root, hash, fmt.Errorf("missing code with hash %x for account %x", hash[:], addr[:])
			}

		case 'S':
			if _, err := io.ReadFull(in, key[:]); err != nil {
				return root, hash, err
			}
			if _, err := io.ReadFull(in, value[:]); err != nil {
				return root, hash, err
			}
			if err := db.SetStorage(addr, key, value); err != nil {
				return root, hash, err
			}

		case 'C':
			code, err := genesis.ReadCode(in)
			if err != nil {
				return root, hash, err
			}
			codes[common.Keccak256(code)] = code
		case 'H':
			if _, err := io.ReadFull(in, buffer); err != nil {
				return root, hash, err
			}
			hashType := genesis.HashType(buffer[0])
			hash := common.Hash{}
			if _, err := io.ReadFull(in, hash[:]); err != nil {
				return root, hash, err
			}
			if hashType == genesis.EthereumHash {
				fmt.Printf("hash %x\n", hash)
				stateHash = hash
				hashFound = true
			}
		default:
			return root, hash, fmt.Errorf("format error encountered, unexpected token type: %c", buffer[0])
		}
	}
}

func checkEmptyDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("failed to open directory %s: %w", directory, err)
	}
	defer file.Close()
	state, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to open file information for %s: %w", directory, err)
	}
	if !state.IsDir() {
		return fmt.Errorf("the path `%s` does not point to a directory", directory)
	}
	_, err = file.Readdirnames(1)
	if err == nil {
		return fmt.Errorf("directory `%s` is not empty", directory)
	}
	if !errors.Is(err, io.EOF) {
		return fmt.Errorf("failed to list content of directory `%s`: %w", directory, err)
	}
	return nil
}
