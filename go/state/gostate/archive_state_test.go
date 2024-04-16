//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3 
//

package gostate

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
	"go.uber.org/mock/gomock"
)

func TestState_ArchiveState_FailingOperation_InvalidatesArchive(t *testing.T) {
	injectedErr := fmt.Errorf("injectedError")
	ctrl := gomock.NewController(t)

	liveDB := state.NewMockLiveDB(ctrl)
	liveDB.EXPECT().Flush().AnyTimes()

	tests := map[string]struct {
		setup  func(archive *archive.MockArchive, injectedErr error)
		action func(stateArchive state.State) error
	}{
		"exists": {
			func(archive *archive.MockArchive, injectedErr error) {
				archive.EXPECT().Exists(gomock.Any(), gomock.Any()).Return(false, injectedErr)
			},
			func(stateArchive state.State) error {
				_, err := stateArchive.Exists(common.Address{})
				return err
			},
		},
		"balance": {
			func(archive *archive.MockArchive, injectedErr error) {
				archive.EXPECT().GetBalance(gomock.Any(), gomock.Any()).Return(common.Balance{}, injectedErr)
			},
			func(stateArchive state.State) error {
				_, err := stateArchive.GetBalance(common.Address{})
				return err
			},
		},
		"code": {
			func(archive *archive.MockArchive, injectedErr error) {
				archive.EXPECT().GetCode(gomock.Any(), gomock.Any()).Return(nil, injectedErr)
			},
			func(stateArchive state.State) error {
				_, err := stateArchive.GetCode(common.Address{})
				return err
			},
		},
		"nonce": {
			func(archive *archive.MockArchive, injectedErr error) {
				archive.EXPECT().GetNonce(gomock.Any(), gomock.Any()).Return(common.Nonce{}, injectedErr)
			},
			func(stateArchive state.State) error {
				_, err := stateArchive.GetNonce(common.Address{})
				return err
			},
		},
		"storage": {
			func(archive *archive.MockArchive, injectedErr error) {
				archive.EXPECT().GetStorage(gomock.Any(), gomock.Any(), gomock.Any()).Return(common.Value{}, injectedErr)
			},
			func(stateArchive state.State) error {
				_, err := stateArchive.GetStorage(common.Address{}, common.Key{})
				return err
			},
		},
		"hash": {
			func(archive *archive.MockArchive, injectedErr error) {
				archive.EXPECT().GetHash(gomock.Any()).Return(common.Hash{}, injectedErr)
			},
			func(stateArchive state.State) error {
				_, err := stateArchive.GetHash()
				return err
			},
		},
		"blockHeight": {
			func(archive *archive.MockArchive, injectedErr error) {
				archive.EXPECT().GetBlockHeight().Return(uint64(0), false, injectedErr)
			},
			func(stateArchive state.State) error {
				_, _, err := stateArchive.GetArchiveBlockHeight()
				return err
			},
		},
	}

	testNames := make([]string, 0, len(tests))
	for k := range tests {
		testNames = append(testNames, k)
	}

	for _, name := range testNames {
		t.Run(fmt.Sprintf("test_%s", name), func(t *testing.T) {
			archiveDB := archive.NewMockArchive(ctrl)

			archive := &ArchiveState{
				archive: archiveDB,
				block:   0,
			}

			// mock methods that until current loop it produces no error,
			// for current loop in injects the error,
			// and interrupt the loop as further methods will not be tested
			// during the test, as they are expected to fail
			for _, subName := range testNames {
				if subName == name {
					tests[subName].setup(archiveDB, injectedErr)
					break
				} else {
					tests[subName].setup(archiveDB, nil)
				}
			}

			// call all methods, all must start to fail from the current position
			var expectedErr error
			for _, subName := range testNames {
				if subName == name {
					expectedErr = injectedErr
				}
				if got, want := tests[subName].action(archive), expectedErr; !errors.Is(got, want) {
					t.Errorf("expected error does not match: %v != %v", got, want)
				}
			}

			// all must fail when called next time
			for _, test := range tests {
				if got, want := test.action(archive), expectedErr; !errors.Is(got, want) {
					t.Errorf("expected error does not match: %v != %v", got, want)
				}
			}

			if err := archive.Check(); !errors.Is(err, injectedErr) {
				t.Errorf("check should fail")
			}
		})
	}
}
