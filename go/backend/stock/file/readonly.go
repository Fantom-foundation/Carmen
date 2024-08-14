// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package file

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/utils"
	"os"
)

type readonly[I stock.Index, V any] struct {
	file    utils.OsFile
	encoder stock.ValueEncoder[V]
	buffer  []byte
}

// OpenReadOnlyStock opens a stock for reading only.
// Multiple read-only stocks may be opened for the same directory at any time. However,
// there are no guarantees against concurrent modifications introduced by Stock instances
// accessing the same underlying files. It is the task of the user of this API to make sure that
// no elements accessed by this reader are modified concurrently to guarantee consistency.
func OpenReadOnlyStock[I stock.Index, V any](directory string, encoder stock.ValueEncoder[V]) (stock.ReadOnly[I, V], error) {
	if err := VerifyStock[I](directory, encoder); err != nil {
		return &readonly[I, V]{}, err
	}

	return openReadOnlyStock[I, V](directory, encoder)
}

// openReadOnlyStock opens a stock for reading only.
func openReadOnlyStock[I stock.Index, V any](directory string, encoder stock.ValueEncoder[V]) (*readonly[I, V], error) {
	file, err := os.Open(fmt.Sprintf("%s/values.dat", directory))
	if err != nil {
		return &readonly[I, V]{}, err
	}

	return &readonly[I, V]{
		file:    file,
		encoder: encoder,
		buffer:  make([]byte, encoder.GetEncodedSize()),
	}, nil
}

func (s *readonly[I, V]) Get(id I) (V, error) {
	var res V
	_, err := s.file.Seek(int64(id)*int64(s.encoder.GetEncodedSize()), 0)
	if err != nil {
		return res, err
	}
	_, err = s.file.Read(s.buffer)
	if err != nil {
		return res, err
	}
	if err := s.encoder.Load(s.buffer, &res); err != nil {
		return res, err
	}
	return res, nil
}

func (s *readonly[I, V]) Close() error {
	return s.file.Close()
}
