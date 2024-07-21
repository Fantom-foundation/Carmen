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
	"hash"
	"io"
	"maps"
	"os"
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
	directory string
	hasher    hash.Hash
}

var emptyCodeHash = common.GetHash(sha3.NewLegacyKeccak256(), []byte{})

func openCodes(file string, directory string) (*codes, error) {
	data, err := readCodes(file)
	if err != nil {
		return nil, err
	}

	// TODO: add support for checkpoint support

	return &codes{
		codes:     data,
		file:      file,
		directory: directory,
		hasher:    sha3.NewLegacyKeccak256(),
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

	err := appendCodes(codes, c.file)
	if err == nil {
		c.pending = c.pending[:0]
	}
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
	return nil // any checkpoint can be restored
}

func (c *codes) Prepare(checkpoint utils.Checkpoint) error {
	return c.Flush() // all that is needed is to make sure that all codes are on the disk
}

func (c *codes) Commit(checkpoint utils.Checkpoint) error {
	return nil // nothing to do
}

func (c *codes) Abort(checkpoint utils.Checkpoint) error {
	return nil // nothing to do
}

func (c *codes) Restore(checkpoint utils.Checkpoint) error {
	return nil // restoration is a no-op
}

// readCodes parses the content of the given file if it exists or returns
// a an empty code collection if there is no such file.
func readCodes(path string) (map[common.Hash][]byte, error) {
	// If there is no file, initialize and return an empty code collection.
	if _, err := os.Stat(path); err != nil {
		return map[common.Hash][]byte{}, nil
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	return parseCodes(reader)
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
func appendCodes(codes map[common.Hash][]byte, filename string) (err error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	return errors.Join(
		writeCodesTo(codes, file),
		file.Close(),
	)
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
