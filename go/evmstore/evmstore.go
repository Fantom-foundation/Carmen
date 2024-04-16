//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public License v3.
//

package evmstore

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/depot"
	fileDepot "github.com/Fantom-foundation/Carmen/go/backend/depot/file"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/backend/index"
	"github.com/Fantom-foundation/Carmen/go/backend/index/file"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/backend/store/pagedfile"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/golang/snappy"
	"os"
	"path/filepath"
	"sync"
)

// receiptsGroupSize represents the number of blocks of receipts grouped together.
const receiptsGroupSize = 4

// txsGroupSize represents the number of txs grouped together.
const txsGroupSize = 1024

// poolSize is the maximum amount of data pages loaded in memory for the paged file store
const poolSize = 1024

type evmStore struct {
	txHashIndex     index.Index[common.Hash, uint64]
	txPositionStore store.Store[uint64, TxPosition]
	txsDepot        depot.Depot[uint64]
	receiptsDepot   depot.Depot[uint64]
	mu              sync.Mutex
}

// Parameters struct defining configuration parameters for EvmStore instances.
type Parameters struct {
	Directory string
}

// NewEvmStore provide a new EvmStore instance
func NewEvmStore(params Parameters) (EvmStore, error) {
	success := false
	txHashPath := params.Directory + string(filepath.Separator) + "txhash"
	TxPositionPath := params.Directory + string(filepath.Separator) + "txpos"
	txsPath := params.Directory + string(filepath.Separator) + "txs"
	receiptsPath := params.Directory + string(filepath.Separator) + "receipts"
	if err := os.MkdirAll(txHashPath, 0700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(TxPositionPath, 0700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(txsPath, 0700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(receiptsPath, 0700); err != nil {
		return nil, err
	}

	txHashIndex, err := file.NewIndex[common.Hash, uint64](txHashPath, common.HashSerializer{}, common.Identifier64Serializer{}, common.HashHasher{}, common.HashComparator{})
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			txHashIndex.Close()
		}
	}()
	txPositionStore, err := pagedfile.NewStore[uint64, TxPosition](TxPositionPath, TxPositionSerializer{}, common.PageSize, hashtree.GetNoHashFactory(), poolSize)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			txPositionStore.Close()
		}
	}()
	txsDepot, err := fileDepot.NewDepot[uint64](txsPath, common.Identifier64Serializer{}, hashtree.GetNoHashFactory(), txsGroupSize)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			txsDepot.Close()
		}
	}()
	receiptsDepot, err := fileDepot.NewDepot[uint64](receiptsPath, common.Identifier64Serializer{}, hashtree.GetNoHashFactory(), receiptsGroupSize)
	if err != nil {
		return nil, err
	}
	success = true
	return &evmStore{
		txHashIndex:     txHashIndex,
		txPositionStore: txPositionStore,
		txsDepot:        txsDepot,
		receiptsDepot:   receiptsDepot,
	}, nil
}

// SetTxPosition stores transaction block and position.
func (s *evmStore) SetTxPosition(txHash common.Hash, position TxPosition) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx, err := s.txHashIndex.GetOrAdd(txHash)
	if err != nil {
		return fmt.Errorf("failed resolve txHash %x; %v", txHash, err)
	}

	err = s.txPositionStore.Set(idx, position)
	if err != nil {
		return fmt.Errorf("failed set txPosition for %x; %v", txHash, err)
	}
	return nil
}

// GetTxPosition returns stored transaction block and position.
// Returns empty position with no err if the tx is not present.
func (s *evmStore) GetTxPosition(txHash common.Hash) (TxPosition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx, err := s.txHashIndex.Get(txHash)
	if err != nil {
		if err == index.ErrNotFound {
			return TxPosition{}, nil
		}
		return TxPosition{}, fmt.Errorf("failed resolve txHash %x; %v", txHash, err)
	}
	txPosition, err := s.txPositionStore.Get(idx)
	if err != nil {
		return TxPosition{}, fmt.Errorf("failed get txPosition for %x; %v", txHash, err)
	}
	return txPosition, nil
}

// SetTx stores non-event transaction.
func (s *evmStore) SetTx(txHash common.Hash, tx []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx, err := s.txHashIndex.GetOrAdd(txHash)
	if err != nil {
		return err
	}

	compressed := snappy.Encode(nil, tx)
	err = s.txsDepot.Set(idx, compressed)
	if err != nil {
		return fmt.Errorf("failed set non-event tx %x; %v", txHash, err)
	}
	return nil
}

// GetTx returns stored non-event transaction.
// Returns nil,nil if the tx is not present.
func (s *evmStore) GetTx(txHash common.Hash) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx, err := s.txHashIndex.Get(txHash)
	if err != nil {
		if err == index.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	compressed, err := s.txsDepot.Get(idx)
	if err != nil {
		return nil, fmt.Errorf("failed to get tx by hash %x; %v", txHash, err)
	}
	if compressed == nil {
		return nil, nil
	}
	tx, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress non-event tx %x; %v", txHash, err)
	}
	return tx, nil
}

// SetRawReceipts stores raw transaction receipts for one block.
func (s *evmStore) SetRawReceipts(block uint64, receipts []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	compressed := snappy.Encode(nil, receipts)
	err := s.receiptsDepot.Set(block, compressed)
	if err != nil {
		return fmt.Errorf("failed to set receipts for block %d; %v", block, err)
	}
	return nil
}

// GetRawReceipts loads raw transaction receipts.
// Returns nil,nil if receipts for given block aren't stored.
func (s *evmStore) GetRawReceipts(block uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	compressed, err := s.receiptsDepot.Get(block)
	if err != nil {
		return nil, fmt.Errorf("failed to get receipts for block %d; %v", block, err)
	}
	if compressed == nil {
		return nil, nil
	}
	receipts, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress receipts for block %d; %v", block, err)
	}
	return receipts, nil
}

func (s *evmStore) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.txHashIndex.Flush()
	if err != nil {
		return err
	}
	err = s.txPositionStore.Flush()
	if err != nil {
		return err
	}
	err = s.txsDepot.Flush()
	if err != nil {
		return err
	}
	err = s.receiptsDepot.Flush()
	if err != nil {
		return err
	}
	return nil
}

func (s *evmStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.txHashIndex.Close()
	if err != nil {
		return err
	}
	err = s.txPositionStore.Close()
	if err != nil {
		return err
	}
	err = s.txsDepot.Close()
	if err != nil {
		return err
	}
	err = s.receiptsDepot.Close()
	if err != nil {
		return err
	}
	return nil
}
