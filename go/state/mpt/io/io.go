package io

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/mpt"
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

// Export opens a LiveDB instance retained in the given directory and writes
// its content to the given output writer. The result contains all the
// information required by the Import function below to reconstruct the full
// state of the LiveDB.
func Export(directory string, out io.Writer) error {

	info, err := CheckMptDirectoryAndGetInfo(directory)
	if err != nil {
		return fmt.Errorf("error in input directory: %v", err)
	}

	if info.Config.Name != mpt.S5LiveConfig.Name {
		return fmt.Errorf("can only support export of LiveDB instances, found %v in directory", info.Mode)
	}

	db, err := mpt.OpenGoFileState(directory, info.Config)
	if err != nil {
		return fmt.Errorf("failed to open LiveDB: %v", err)
	}
	defer db.Close()

	// Start with the magic number.
	if _, err := out.Write(stateMagicNumber); err != nil {
		return err
	}

	// Add a version number.
	if _, err := out.Write([]byte{formatVersion}); err != nil {
		return err
	}

	// Continue with the full state hash.
	hash, err := db.GetHash()
	if err != nil {
		return err
	}
	if _, err := out.Write([]byte{byte('H'), byte(EthereumHash)}); err != nil {
		return err
	}
	if _, err := out.Write(hash[:]); err != nil {
		return err
	}

	// Write out codes.
	codes, err := db.GetCodes()
	if err != nil {
		return fmt.Errorf("failed to retrieve codes: %v", err)
	}
	for _, code := range codes {
		b := []byte{byte('C'), 0, 0}
		binary.BigEndian.PutUint16(b[1:], uint16(len(code)))
		if _, err := out.Write(b); err != nil {
			return fmt.Errorf("output error: %v", err)
		}
		if _, err := out.Write(code); err != nil {
			return fmt.Errorf("output error: %v", err)
		}
	}

	// Write out all accounts and values.
	visitor := exportVisitor{out: out}
	if err := db.Visit(&visitor); err != nil || visitor.err != nil {
		return fmt.Errorf("failed exporting content: %v", errors.Join(err, visitor.err))
	}

	return nil
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
			NodeId: mpt.EmptyId(),
			Hash:   mpt.EmptyNodeEthereumHash,
		}
	}
	roots[block] = mpt.Root{
		NodeId: root,
		Hash:   hash,
	}
	if err := mpt.StoreRoots(directory+string(os.PathSeparator)+"roots.dat", roots); err != nil {
		return err
	}
	return nil
}

func runImport(directory string, in io.Reader, config mpt.MptConfig) (root mpt.NodeId, hash common.Hash, err error) {
	// Start by checking the magic number.
	buffer := make([]byte, len(stateMagicNumber))
	if _, err := io.ReadFull(in, buffer); err != nil {
		return root, hash, err
	} else if !bytes.Equal(buffer, stateMagicNumber) {
		return root, hash, fmt.Errorf("invalid format, wrong magic number")
	}

	// Check the version number.
	if _, err := io.ReadFull(in, buffer[0:1]); err != nil {
		return root, hash, err
	} else if buffer[0] != formatVersion {
		return root, hash, fmt.Errorf("invalid format, unsupported version")
	}

	// Create a state.
	db, err := mpt.OpenGoFileState(directory, config)
	if err != nil {
		return root, hash, fmt.Errorf("failed to create empty state: %v", err)
	}
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	var (
		addr    common.Address
		key     common.Key
		value   common.Value
		balance common.Balance
		nonce   common.Nonce
	)
	length := []byte{0, 0}

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
			if err := db.SetBalance(addr, balance); err != nil {
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
			if _, err := io.ReadFull(in, length[:]); err != nil {
				return root, hash, err
			}
			code := make([]byte, binary.BigEndian.Uint16(length))
			if _, err := io.ReadFull(in, code); err != nil {
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
		}
	}
}

// exportVisitor is an internal utility used by the Export function to write
// account and value node information to a given output writer.
type exportVisitor struct {
	out io.Writer
	err error
}

func (e *exportVisitor) Visit(node mpt.Node, _ mpt.NodeInfo) mpt.VisitResponse {
	switch n := node.(type) {
	case *mpt.AccountNode:
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
		if _, err := e.out.Write(info.Balance[:]); err != nil {
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
