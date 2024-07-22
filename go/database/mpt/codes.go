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
	"encoding/json"
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
	"github.com/Fantom-foundation/Carmen/go/common"
	"golang.org/x/crypto/sha3"
)

type codes struct {
	codes     map[common.Hash][]byte
	pending   []common.Hash
	mutex     sync.Mutex
	file      string
	fileSize  uint64
	directory string
	hasher    hash.Hash

	checkpoint         utils.Checkpoint
	checkpointFileSize uint64
}

var emptyCodeHash = common.GetHash(sha3.NewLegacyKeccak256(), []byte{})

func openCodes(file string, directory string) (*codes, error) {
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

	committed := filepath.Join(directory, "committed.json")
	meta, err := readCodeCheckpointMetaData(committed)
	if err != nil {
		return nil, err
	}

	return &codes{
		codes:              data,
		file:               file,
		fileSize:           size,
		directory:          directory,
		hasher:             sha3.NewLegacyKeccak256(),
		checkpoint:         meta.Checkpoint,
		checkpointFileSize: meta.FileSize,
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

func (c *codes) GuaranteeCheckpoint(checkpoint utils.Checkpoint) error {
	if c.checkpoint == checkpoint {
		return nil
	}

	if c.checkpoint+1 == checkpoint {
		preparedFile := filepath.Join(c.directory, "prepare.json")
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

func (c *codes) Prepare(checkpoint utils.Checkpoint) error {
	if c.checkpoint+1 != checkpoint {
		return fmt.Errorf("cannot prepare checkpoint %d, current checkpoint is %d", checkpoint, c.checkpoint)
	}
	if err := c.Flush(); err != nil {
		return err
	}
	preparedFile := filepath.Join(c.directory, "prepare.json")
	return writeCodeCheckpointMetaData(preparedFile, codeCheckpointMetaData{
		Checkpoint: checkpoint,
		FileSize:   c.fileSize,
	})
}

func (c *codes) Commit(checkpoint utils.Checkpoint) error {
	committedFile := filepath.Join(c.directory, "committed.json")
	preparedFile := filepath.Join(c.directory, "prepare.json")
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
	c.checkpointFileSize = meta.FileSize
	return nil
}

func (c *codes) Abort(checkpoint utils.Checkpoint) error {
	return os.Remove(filepath.Join(c.directory, "prepare.json"))
}

func (c *codes) Restore(checkpoint utils.Checkpoint) error {
	committedFile := filepath.Join(c.directory, "committed.json")
	meta, err := readCodeCheckpointMetaData(committedFile)
	if err != nil {
		return err
	}
	if meta.Checkpoint != checkpoint {
		return fmt.Errorf("cannot restore checkpoint %d, committed checkpoint is %d", checkpoint, meta.Checkpoint)
	}
	c.checkpoint = checkpoint
	c.checkpointFileSize = meta.FileSize

	if err := os.Truncate(c.file, int64(c.checkpointFileSize)); err != nil {
		return err
	}

	c.codes, c.fileSize, err = readCodesAndSize(c.file)
	c.pending = c.pending[:0]
	return err
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
	if err != nil {
		return map[common.Hash][]byte{}, 0, nil
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
	return errors.Join(
		writeCodesTo(codes, file),
		file.Close(),
	)
}

// appendCodes appends the given map of codes to the given file.
func appendCodes(codes map[common.Hash][]byte, filename string) (fileSize uint64, err error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return 0, err
	}
	if err := writeCodesTo(codes, file); err != nil {
		return 0, err
	}
	size, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	return uint64(size), file.Close()
}

func writeCodesTo(codes map[common.Hash][]byte, out io.Writer) (err error) {
	writer := bufio.NewWriter(out)
	// The format is simple: [<key>, <length>, <code>]*
	for key, code := range codes {
		if _, err := writer.Write(key[:]); err != nil {
			return err
		}
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(code)))
		if _, err := writer.Write(length[:]); err != nil {
			return err
		}
		if _, err := writer.Write(code); err != nil {
			return err
		}
	}
	return writer.Flush()
}

type codeCheckpointMetaData struct {
	Checkpoint utils.Checkpoint
	FileSize   uint64
}

func readCodeCheckpointMetaData(path string) (codeCheckpointMetaData, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return codeCheckpointMetaData{}, nil
	}
	if err != nil {
		return codeCheckpointMetaData{}, err
	}
	var meta codeCheckpointMetaData
	err = json.Unmarshal(data, &meta)
	return meta, err
}

func writeCodeCheckpointMetaData(path string, meta codeCheckpointMetaData) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}
