package io

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/Fantom-foundation/Carmen/go/common"
)

func writeCodes(codes map[common.Hash][]byte, out io.Writer) error {
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
	return nil
}

func readCode(in io.Reader) ([]byte, error) {
	length := []byte{0, 0}
	if _, err := io.ReadFull(in, length[:]); err != nil {
		return nil, err
	}
	code := make([]byte, binary.BigEndian.Uint16(length))
	if _, err := io.ReadFull(in, code); err != nil {
		return nil, err
	}
	return code, nil
}
