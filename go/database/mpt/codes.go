// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package mpt

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/utils"
	"github.com/Fantom-foundation/Carmen/go/backend/utils/checkpoint"
	"github.com/Fantom-foundation/Carmen/go/common"
	"golang.org/x/crypto/sha3"
)

// codes is a simple data structure to store and manage the codes of accounts.
// All codes are retained in memory, incrementally backed up to disk during
// checkpoint and flush operations.
type codes struct {
	codes    map[common.Hash][]byte // < all managed codes
	pending  []common.Hash          // < the hashes of codes not written to disk yet
	file     string                 // < the file to store the codes
	fileSize uint64                 // < the current file size
	mutex    sync.Mutex
	hasher   hash.Hash

	directory  string                // < a directory for placing checkpoint data
	checkpoint checkpoint.Checkpoint // < the last checkpoint
}

var emptyCodeHash = common.GetHash(sha3.NewLegacyKeccak256(), []byte{})

const (
	fileNameCodes                    = "codes.dat"
	fileNameCodesCheckpointDirectory = "codes"
	fileNameCodesCommittedCheckpoint = "committed.json"
	fileNameCodesPrepareCheckpoint   = "prepare.json"
)

func openCodes(stateDirectory string) (*codes, error) {
	file, directory := getCodePaths(stateDirectory)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}

	// Create the code file if it does not exist.
	if _, err := os.Stat(file); os.IsNotExist(err) {
		if err := os.WriteFile(file, []byte{}, 0600); err != nil {
			return nil, err
		}
	}

	data, size, err := readCodesAndSize(file)
	if err != nil {
		return nil, err
	}

	committed := filepath.Join(directory, fileNameCodesCommittedCheckpoint)
	meta, err := readCodeCheckpointMetaData(committed)
	if err != nil {
		return nil, err
	}

	return &codes{
		codes:      data,
		file:       file,
		fileSize:   size,
		directory:  directory,
		hasher:     sha3.NewLegacyKeccak256(),
		checkpoint: meta.Checkpoint,
	}, nil
}

func (c *codes) add(code []byte) common.Hash {
	hash := common.GetHash(c.hasher, code)
	c.mutex.Lock()
	if _, found := c.codes[hash]; !found {
		c.codes[hash] = code
		c.pending = append(c.pending, hash)
	}
	c.mutex.Unlock()
	return hash
}

func (c *codes) getCodeForHash(hash common.Hash) []byte {
	c.mutex.Lock()
	res := c.codes[hash]
	c.mutex.Unlock()
	return res
}

func (c *codes) getCodes() map[common.Hash][]byte {
	c.mutex.Lock()
	res := maps.Clone(c.codes)
	c.mutex.Unlock()
	return res
}

func (c *codes) Flush() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.pending) == 0 {
		return nil
	}
	codes := make(map[common.Hash][]byte, len(c.pending))
	for _, hash := range c.pending {
		codes[hash] = c.codes[hash]
	}

	size, err := appendCodes(codes, c.file)
	if err == nil {
		c.pending = c.pending[:0]
	}
	c.fileSize = size
	return err
}

func (c *codes) GetMemoryFootprint() *common.MemoryFootprint {
	var sizeCodes uint
	c.mutex.Lock()
	for k, v := range c.codes {
		sizeCodes += uint(len(k) + len(v))
	}
	c.mutex.Unlock()
	return common.NewMemoryFootprint(unsafe.Sizeof(*c) + uintptr(sizeCodes))
}

func (c *codes) GuaranteeCheckpoint(checkpoint checkpoint.Checkpoint) error {
	if c.checkpoint == checkpoint {
		return nil
	}

	if c.checkpoint+1 == checkpoint {
		preparedFile := filepath.Join(c.directory, fileNameCodesPrepareCheckpoint)
		meta, err := readCodeCheckpointMetaData(preparedFile)
		if err != nil {
			return err
		}
		if meta.Checkpoint == checkpoint {
			return c.Commit(checkpoint)
		}
	}

	return fmt.Errorf("cannot guarantee checkpoint %d, current checkpoint is %d", checkpoint, c.checkpoint)
}

func (c *codes) Prepare(checkpoint checkpoint.Checkpoint) error {
	if c.checkpoint+1 != checkpoint {
		return fmt.Errorf("cannot prepare checkpoint %d, current checkpoint is %d", checkpoint, c.checkpoint)
	}
	if err := c.Flush(); err != nil {
		return err
	}
	preparedFile := filepath.Join(c.directory, fileNameCodesPrepareCheckpoint)
	return writeCodeCheckpointMetaData(preparedFile, codeCheckpointMetaData{
		Checkpoint: checkpoint,
		FileSize:   c.fileSize,
	})
}

func (c *codes) Commit(checkpoint checkpoint.Checkpoint) error {
	committedFile := filepath.Join(c.directory, fileNameCodesCommittedCheckpoint)
	preparedFile := filepath.Join(c.directory, fileNameCodesPrepareCheckpoint)
	meta, err := readCodeCheckpointMetaData(preparedFile)
	if err != nil {
		return err
	}
	if meta.Checkpoint != checkpoint {
		return fmt.Errorf("cannot commit checkpoint %d, prepared checkpoint is %d", checkpoint, meta.Checkpoint)
	}
	if err := os.Rename(preparedFile, committedFile); err != nil {
		return err
	}
	c.checkpoint = checkpoint
	return nil
}

func (c *codes) Abort(checkpoint checkpoint.Checkpoint) error {
	return os.Remove(filepath.Join(c.directory, fileNameCodesPrepareCheckpoint))
}

func getCodePaths(directory string) (codeFile, codeDir string) {
	return filepath.Join(directory, fileNameCodes),
		filepath.Join(directory, fileNameCodesCheckpointDirectory)
}

type codeRestorer struct {
	file      string
	directory string
}

func getCodeRestorer(stateDirectory string) codeRestorer {
	file, directory := getCodePaths(stateDirectory)
	return codeRestorer{
		file:      file,
		directory: directory,
	}
}

func (r codeRestorer) Restore(checkpoint checkpoint.Checkpoint) error {
	committedFile := filepath.Join(r.directory, fileNameCodesCommittedCheckpoint)
	meta, err := readCodeCheckpointMetaData(committedFile)
	if err != nil {
		return err
	}
	if meta.Checkpoint != checkpoint {
		return fmt.Errorf("cannot restore checkpoint %d, committed checkpoint is %d", checkpoint, meta.Checkpoint)
	}
	return os.Truncate(r.file, int64(meta.FileSize))
}

// readCodes parses the content of the given file if it exists or returns
// a an empty code collection if there is no such file.
func readCodes(path string) (map[common.Hash][]byte, error) {
	codes, _, err := readCodesAndSize(path)
	return codes, err
}

// readCodesAndSize parses the content of the given file and returns the
// contained collection of codes and the size of the file.
func readCodesAndSize(path string) (map[common.Hash][]byte, uint64, error) {
	// If there is no file, initialize and return an empty code collection.
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return map[common.Hash][]byte{}, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	data, err := parseCodes(reader)
	return data, uint64(info.Size()), err
}

func parseCodes(reader io.Reader) (map[common.Hash][]byte, error) {
	// If the file exists, parse it and return its content.
	res := map[common.Hash][]byte{}
	// The format is simple: [<key>, <length>, <code>]*
	var hash common.Hash
	var length [4]byte
	for {
		if _, err := io.ReadFull(reader, hash[:]); err != nil {
			if err == io.EOF {
				return res, nil
			}
			return nil, err
		}
		if _, err := io.ReadFull(reader, length[:]); err != nil {
			return nil, err
		}
		size := binary.BigEndian.Uint32(length[:])
		code := make([]byte, size)
		if _, err := io.ReadFull(reader, code[:]); err != nil {
			return nil, err
		}
		res[hash] = code
	}
}

// writeCodes write the given map of codes to the given file.
func writeCodes(codes map[common.Hash][]byte, filename string) (err error) {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	buffer := bufio.NewWriter(file)
	return errors.Join(
		writeCodesTo(codes, buffer),
		buffer.Flush(),
		file.Close(),
	)
}

// appendCodes appends the given map of codes to the given file.
func appendCodes(codes map[common.Hash][]byte, filename string) (fileSize uint64, err error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return 0, err
	}
	buffer := bufio.NewWriter(file)
	err1 := writeCodesTo(codes, buffer)
	err2 := buffer.Flush()
	size, err3 := file.Seek(0, io.SeekCurrent)
	return uint64(size), errors.Join(err1, err2, err3, file.Close())
}

func writeCodesTo(codes map[common.Hash][]byte, out io.Writer) (err error) {
	// The format is simple: [<key>, <length>, <code>]*
	for key, code := range codes {
		if _, err := out.Write(key[:]); err != nil {
			return err
		}
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(code)))
		if _, err := out.Write(length[:]); err != nil {
			return err
		}
		if _, err := out.Write(code); err != nil {
			return err
		}
	}
	return nil
}

type codeCheckpointMetaData struct {
	Checkpoint checkpoint.Checkpoint
	FileSize   uint64
}

func readCodeCheckpointMetaData(path string) (codeCheckpointMetaData, error) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return codeCheckpointMetaData{}, nil
	}
	return utils.ReadJsonFile[codeCheckpointMetaData](path)
}

func writeCodeCheckpointMetaData(path string, meta codeCheckpointMetaData) error {
	return utils.WriteJsonFile(path, meta)
}
