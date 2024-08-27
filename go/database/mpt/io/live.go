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
	"github.com/Fantom-foundation/Carmen/go/common/interrupt"
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

var stateMagicNumber []byte = []byte("Fantom-World-State")

const formatVersion = byte(1)

type HashType byte

// So far there is only one hash type supported, the Ethereum hash. But for
// future situations we might want to support different hash types, like the
// S4 hash definition. Thus this enum is introduced as a placeholder.
const (
	EthereumHash = HashType(0)
)

// mptStateVisitor is an interface for Tries that allows for visiting the Trie nodes
// and furthermore getting its properties such as a root hash and contract codes.
type mptStateVisitor interface {
	// Visit allows for traverse the whole trie.
	Visit(visitor mpt.NodeVisitor, cutAtAccounts bool) error
	// GetHash returns the hash of the represented Trie.
	GetHash() (common.Hash, error)
	// GetCodeForHash returns byte code for given hash.
	GetCodeForHash(common.Hash) []byte
}

type exportableArchiveTrie struct {
	directory string
	trie      *mpt.ArchiveTrie
	block     uint64
}

func (e exportableArchiveTrie) Visit(visitor mpt.NodeVisitor, cutAtAccounts bool) error {
	root, err := e.trie.GetBlockRoot(e.block)
	if err != nil {
		return err
	}

	return visitAll(&nodeSourceFactory{directory: e.directory}, root, visitor, cutAtAccounts)
}

func (e exportableArchiveTrie) GetHash() (common.Hash, error) {
	return e.trie.GetHash(e.block)
}

func (e exportableArchiveTrie) GetCodeForHash(hash common.Hash) []byte {
	return e.trie.GetCodeForHash(hash)
}

// Export opens a LiveDB instance retained in the given directory and writes
// its content to the given output writer. The result contains all the
// information required by the Import function below to reconstruct the full
// state of the LiveDB.
func Export(ctx context.Context, logger *Log, directory string, out io.Writer) error {
	info, err := CheckMptDirectoryAndGetInfo(directory)
	if err != nil {
		return fmt.Errorf("error in input directory: %v", err)
	}

	if info.Config.Name != mpt.S5LiveConfig.Name {
		return fmt.Errorf("can only support export of LiveDB instances, found %v in directory", info.Mode)
	}

	logger.Printf("opening liveDb: %s", directory)
	db, err := mpt.OpenGoFileState(directory, info.Config, mpt.NodeCacheConfig{})
	if err != nil {
		return fmt.Errorf("failed to open LiveDB: %v", err)
	}
	defer db.Close()

	_, err = ExportLive(ctx, logger, db, out)
	return err
}

// ExportBlockFromArchive exports LiveDB genesis for a single given block from an Archive.
// Note: block must be <= of Archive block height.
func ExportBlockFromArchive(ctx context.Context, logger *Log, directory string, out io.Writer, block uint64) error {
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

	defer archive.Close()
	_, err = ExportLive(ctx, logger, exportableArchiveTrie{
		directory: directory,
		trie:      archive,
		block:     block,
	}, out)
	return err
}

// ExportBlockFromOnlineArchive exports LiveDB view for a single given block from an Archive.
// This method exports from the online archive, i.e, an archive that is being updated with new blocks.
// To ensure the exported data are up-to-date, this method flushes the archive to disk before exporting.
// For this reason, this method should not be called often, as it would hamper performance.
// Expected usage is, for instance, the creation of a LiveDB state dumps to back up the state of selected blocks for fast recovery.
func ExportBlockFromOnlineArchive(ctx context.Context, archive *mpt.ArchiveTrie, out io.Writer, block uint64) error {
	logger := NewLog()
	logger.Printf("exporting block %d from online archive", block)
	defer func() {
		logger.Printf("exported block %d from online archive", block)
	}()

	logger.Printf("flushing archive")
	// before doing anything, flush the archive to ensure the data is up-to-date
	if err := archive.Flush(); err != nil {
		return err
	}

	logger.Printf("exporting")
	_, err := ExportLive(ctx, logger, exportableArchiveTrie{
		directory: archive.Directory(),
		trie:      archive,
		block:     block,
	}, out)
	return err
}

// ExportBlockFromOnlineArchive exports a LiveDB dump for a single given block from an Archive.
// This method exports from an online archive, i.e, an archive that is being updated with new blocks.
// To ensure the exported data is up-to-date, this method flushes the archive to disk before exporting.
// Expected usage is, for instance, the creation of database dumps once in many blocks to backup the state.
func ExportBlockFromOnlineArchive(ctx context.Context, logger *Log, archive *mpt.ArchiveTrie, out io.Writer, block uint64) error {
	logger.Printf("exporting block %d from online archive", block)
	defer func() {
		logger.Printf("exported block %d from online archive", block)
	}()

	logger.Printf("flushing archive")
	// before doing anything, flush the archive to ensure the data is up-to-date
	if err := archive.Flush(); err != nil {
		return err
	}

	logger.Printf("exporting")
	_, err := ExportLive(ctx, logger, exportableArchiveTrie{
		trie:  archive,
		block: block,
	}, out)
	return err
}

// ExportLive exports given db into out.
func ExportLive(ctx context.Context, logger *Log, db mptStateVisitor, out io.Writer) (common.Hash, error) {
	// Start with the magic number.
	if _, err := out.Write(stateMagicNumber); err != nil {
		return common.Hash{}, err
	}

	// Add a version number.
	if _, err := out.Write([]byte{formatVersion}); err != nil {
		return common.Hash{}, err
	}

	// Continue with the full state hash.
	hash, err := db.GetHash()
	if err != nil {
		return common.Hash{}, err
	}
	if _, err := out.Write([]byte{byte('H'), byte(EthereumHash)}); err != nil {
		return common.Hash{}, err
	}
	if _, err := out.Write(hash[:]); err != nil {
		return common.Hash{}, err
	}

	// Write out codes.
	logger.Print("exporting codes")
	codes, err := getReferencedCodes(ctx, logger, db)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to retrieve codes: %w", err)
	}
	if err := writeCodes(codes, out); err != nil {
		return common.Hash{}, err
	}

	// Write out all accounts and values.
	logger.Print("exporting accounts and values")
	progress := logger.NewProgressTracker("exported %d accounts, %.2f accounts/s", 1_000_000)
	visitor := exportVisitor{out: out, ctx: ctx, progress: progress}
	if err := db.Visit(&visitor, false); err != nil || visitor.err != nil {
		return common.Hash{}, fmt.Errorf("failed exporting content: %w", errors.Join(err, visitor.err))
	}

	return hash, nil
}

// ImportLiveDb creates a fresh StateDB in the given directory and fills it
// with the content read from the given reader.
func ImportLiveDb(logger *Log, directory string, in io.Reader) error {
	_, _, err := runImport(logger, directory, in, mpt.S5LiveConfig)
	return err
}

// InitializeArchive creates a fresh Archive in the given directory containing
// the state read from the input stream at the given block. All states before
// the given block are empty.
func InitializeArchive(logger *Log, directory string, in io.Reader, block uint64) (err error) {
	// The import creates a live-DB state that initializes the Archive.
	root, hash, err := runImport(logger, directory, in, mpt.S5ArchiveConfig)
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

func runImport(logger *Log, directory string, in io.Reader, config mpt.MptConfig) (root mpt.NodeId, hash common.Hash, err error) {
	// check that the destination directory is an empty directory
	if err := checkEmptyDirectory(directory); err != nil {
		return root, hash, err
	}

	// Start by checking the magic number.
	buffer := make([]byte, len(stateMagicNumber))
	if _, err := io.ReadFull(in, buffer); err != nil {
		return root, hash, err
	} else if !bytes.Equal(buffer, stateMagicNumber) {
		// Provide an explicit warning to the user if instead of a live state dump an archive dump was provided
		if bytes.Contains(buffer, archiveMagicNumber[:len(stateMagicNumber)]) {
			return root, hash, fmt.Errorf("incorrect input data format use the `import-archive` sub-command  with this type of data")
		}
		return root, hash, errors.New("invalid format, unknown magic number")
	}

	// Check the version number.
	if _, err := io.ReadFull(in, buffer[0:1]); err != nil {
		return root, hash, err
	} else if buffer[0] != formatVersion {
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

	progress := logger.NewProgressTracker("imported %d accounts, %.2f accounts/s", 1_000_000)
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
			progress.Step(1)
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
			code, err := readCode(in)
			if err != nil {
				return root, hash, err
			}
			codes[common.Keccak256(code)] = code
		case 'H':
			if _, err := io.ReadFull(in, buffer); err != nil {
				return root, hash, err
			}
			hashType := HashType(buffer[0])
			hash := common.Hash{}
			if _, err := io.ReadFull(in, hash[:]); err != nil {
				return root, hash, err
			}
			if hashType == EthereumHash {
				stateHash = hash
				hashFound = true
			}
		default:
			return root, hash, fmt.Errorf("format error encountered, unexpected token type: %c", buffer[0])
		}
	}
}

// getReferencedCodes returns a map of codes referenced by accounts in the
// given database. The map is indexed by the code hash.
func getReferencedCodes(ctxt context.Context, logger *Log, db mptStateVisitor) (map[common.Hash][]byte, error) {
	progress := logger.NewProgressTracker("retrieved %d accounts, %.2f accounts/s", 1000_000)
	codes := make(map[common.Hash][]byte)
	err := db.Visit(mpt.MakeVisitor(func(node mpt.Node, info mpt.NodeInfo) mpt.VisitResponse {
		if n, ok := node.(*mpt.AccountNode); ok {
			if interrupt.IsCancelled(ctxt) {
				return mpt.VisitResponseAbort
			}
			progress.Step(1)
			codeHash := n.Info().CodeHash
			code := db.GetCodeForHash(codeHash)
			if len(code) > 0 {
				codes[codeHash] = code
			}
			return mpt.VisitResponsePrune // < no need to visit the storage trie
		}
		return mpt.VisitResponseContinue
	}), true)

	if interrupt.IsCancelled(ctxt) {
		err = interrupt.ErrCanceled
	}
	return codes, err
}

// exportVisitor is an internal utility used by the Export function to write
// account and value node information to a given output writer.
type exportVisitor struct {
	out      io.Writer
	err      error
	ctx      context.Context
	progress *ProgressLogger
}

func (e *exportVisitor) Visit(node mpt.Node, _ mpt.NodeInfo) mpt.VisitResponse {
	// outside call to interrupt
	if interrupt.IsCancelled(e.ctx) {
		e.err = interrupt.ErrCanceled
		return mpt.VisitResponseAbort
	}
	switch n := node.(type) {
	case *mpt.AccountNode:
		e.progress.Step(1)
		addr := n.Address()
		info := n.Info()
		if _, err := e.out.Write([]byte{byte('A')}); err != nil {
			e.err = err
			return mpt.VisitResponseAbort
		}
		if _, err := e.out.Write(addr[:]); err != nil {
			e.err = err
			return mpt.VisitResponseAbort
		}
		b := info.Balance.Bytes32()
		if _, err := e.out.Write(b[:]); err != nil {
			e.err = err
			return mpt.VisitResponseAbort
		}
		if _, err := e.out.Write(info.Nonce[:]); err != nil {
			e.err = err
			return mpt.VisitResponseAbort
		}
		if _, err := e.out.Write(info.CodeHash[:]); err != nil {
			e.err = err
			return mpt.VisitResponseAbort
		}
	case *mpt.ValueNode:
		key := n.Key()
		value := n.Value()
		if _, err := e.out.Write([]byte{byte('S')}); err != nil {
			e.err = err
			return mpt.VisitResponseAbort
		}
		if _, err := e.out.Write(key[:]); err != nil {
			e.err = err
			return mpt.VisitResponseAbort
		}
		if _, err := e.out.Write(value[:]); err != nil {
			e.err = err
			return mpt.VisitResponseAbort
		}
	}
	return mpt.VisitResponseContinue
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
