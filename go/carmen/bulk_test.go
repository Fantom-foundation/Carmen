// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package carmen

import (
	"sync"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/state"
	"go.uber.org/mock/gomock"
)

func TestBulkLoad_Cannot_Finalise_Twice(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockBulkLoad(ctrl)
	stateDB.EXPECT().Close()
	st := state.NewMockState(ctrl)

	bulk := &bulkLoad{
		db: &database{
			db:   st,
			lock: sync.Mutex{},
		},
		nested: stateDB,
	}

	if err := bulk.Finalize(); err != nil {
		t.Errorf("cannot finalise block: %v", err)
	}

	if err := bulk.Finalize(); err == nil {
		t.Errorf("second call to finalise should fail")
	}
}

func TestBulkLoad_Operations_Passthrough(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockBulkLoad(ctrl)
	stateDB.EXPECT().CreateAccount(gomock.Any())
	stateDB.EXPECT().SetCode(gomock.Any(), gomock.Any())
	stateDB.EXPECT().SetNonce(gomock.Any(), gomock.Any())
	stateDB.EXPECT().SetBalance(gomock.Any(), gomock.Any())
	stateDB.EXPECT().SetState(gomock.Any(), gomock.Any(), gomock.Any())
	stateDB.EXPECT().Close()
	st := state.NewMockState(ctrl)

	bulk := &bulkLoad{
		db: &database{
			db:   st,
			lock: sync.Mutex{},
		},
		nested: stateDB,
	}

	bulk.CreateAccount(Address{})
	bulk.SetCode(Address{}, []byte{})
	bulk.SetNonce(Address{}, 10)
	bulk.SetBalance(Address{}, NewAmount(300))
	bulk.SetState(Address{}, Key{}, Value{})

	if err := bulk.Finalize(); err != nil {
		t.Errorf("cannot finalise block: %v", err)
	}

}

func TestBulkLoad_WriteOperationsOnFinalisedInstanceAreNoops(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateDB := state.NewMockBulkLoad(ctrl)
	stateDB.EXPECT().Close()
	st := state.NewMockState(ctrl)

	bulk := &bulkLoad{
		db: &database{
			db:   st,
			lock: sync.Mutex{},
		},
		nested: stateDB,
	}

	if err := bulk.Finalize(); err != nil {
		t.Errorf("cannot finalise block: %v", err)
	}

	bulk.CreateAccount(Address{})
	bulk.SetCode(Address{}, []byte{})
	bulk.SetNonce(Address{}, 10)
	bulk.SetBalance(Address{}, NewAmount(300))
	bulk.SetState(Address{}, Key{}, Value{})

}
