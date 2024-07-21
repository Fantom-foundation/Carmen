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

	"github.com/Fantom-foundation/Carmen/go/common"
	"golang.org/x/crypto/sha3"
)

type codes struct {
	codes     map[common.Hash][]byte
	dirty     bool
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
	c.codes[hash] = code
	c.dirty = true
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
	if !c.dirty {
		return nil
	}
	err := writeCodes(c.codes, c.file)
	if err != nil {
		c.dirty = false
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
	writer := bufio.NewWriter(file)
	return errors.Join(
		writeCodesTo(codes, writer),
		writer.Flush(),
		file.Close())
}

func writeCodesTo(codes map[common.Hash][]byte, writer io.Writer) (err error) {
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
	return nil
}
