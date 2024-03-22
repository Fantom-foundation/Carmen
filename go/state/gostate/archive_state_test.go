package gostate

import (
	"errors"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/archive"
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
	"go.uber.org/mock/gomock"
	"testing"
)

func TestState_ArchiveState_FailingOperation_InvalidatesArchive(t *testing.T) {
	injectedErr := fmt.Errorf("injectedError")
	ctrl := gomock.NewController(t)

	liveDB := NewMockLiveDB(ctrl)
	liveDB.EXPECT().Flush().AnyTimes()

	mocks := []func(archive *archive.MockArchive, injectedErr error){
		func(archive *archive.MockArchive, injectedErr error) {
			archive.EXPECT().GetBlockHeight().Return(uint64(0), false, injectedErr)
		},
		func(archive *archive.MockArchive, injectedErr error) {
			archive.EXPECT().Exists(gomock.Any(), gomock.Any()).Return(false, injectedErr)
		},
		func(archive *archive.MockArchive, injectedErr error) {
			archive.EXPECT().GetBalance(gomock.Any(), gomock.Any()).Return(common.Balance{}, injectedErr)
		},
		func(archive *archive.MockArchive, injectedErr error) {
			archive.EXPECT().GetCode(gomock.Any(), gomock.Any()).Return(nil, injectedErr)
		},
		func(archive *archive.MockArchive, injectedErr error) {
			archive.EXPECT().GetNonce(gomock.Any(), gomock.Any()).Return(common.Nonce{}, injectedErr)
		},
		func(archive *archive.MockArchive, injectedErr error) {
			archive.EXPECT().GetStorage(gomock.Any(), gomock.Any(), gomock.Any()).Return(common.Value{}, injectedErr)
		},
		func(archive *archive.MockArchive, injectedErr error) {
			archive.EXPECT().GetHash(gomock.Any()).Return(common.Hash{}, injectedErr)
		},
	}

	calls := []func(stateArchive state.State, expectedErr error){
		func(stateArchive state.State, expectedErr error) {
			if _, _, err := stateArchive.GetArchiveBlockHeight(); !errors.Is(err, expectedErr) {
				t.Errorf("unexpectedErr: %v != %v", err, expectedErr)
			}
		},
		func(stateArchive state.State, expectedErr error) {
			if _, err := stateArchive.Exists(common.Address{}); !errors.Is(err, expectedErr) {
				t.Errorf("unexpectedErr: %v != %v", err, expectedErr)
			}
		},
		func(stateArchive state.State, expectedErr error) {
			if _, err := stateArchive.GetBalance(common.Address{}); !errors.Is(err, expectedErr) {
				t.Errorf("unexpectedErr: %v != %v", err, expectedErr)
			}
		},
		func(stateArchive state.State, expectedErr error) {
			if _, err := stateArchive.GetCode(common.Address{}); !errors.Is(err, expectedErr) {
				t.Errorf("unexpectedErr: %v != %v", err, expectedErr)
			}
		},
		func(stateArchive state.State, expectedErr error) {
			if _, err := stateArchive.GetNonce(common.Address{}); !errors.Is(err, expectedErr) {
				t.Errorf("unexpectedErr: %v != %v", err, expectedErr)
			}
		},
		func(stateArchive state.State, expectedErr error) {
			if _, err := stateArchive.GetStorage(common.Address{}, common.Key{}); !errors.Is(err, expectedErr) {
				t.Errorf("unexpectedErr: %v != %v", err, expectedErr)
			}
		},
		func(stateArchive state.State, expectedErr error) {
			if _, err := stateArchive.GetHash(); !errors.Is(err, expectedErr) {
				t.Errorf("unexpectedErr: %v != %v", err, expectedErr)
			}
		},
	}

	if got, want := len(mocks), len(calls); got != want {
		t.Fatalf("misconfiguration: %d != %d", got, want)
	}

	for i := 0; i < len(calls); i++ {
		t.Run(fmt.Sprintf("operation_%d", i), func(t *testing.T) {
			archiveDB := archive.NewMockArchive(ctrl)

			archive := &ArchiveState{
				archive: archiveDB,
				block:   0,
			}

			for j, mock := range mocks {
				if i == j {
					mock(archiveDB, injectedErr)
					break
				} else {
					mock(archiveDB, nil)
				}
			}

			// call all methods, all must start to fail from the current position
			var expectedErr error
			for j, call := range calls {
				if i == j {
					expectedErr = injectedErr
				}
				call(archive, expectedErr)
			}

			// all must fail when called next time
			for _, call := range calls {
				call(archive, injectedErr)
			}

			if err := archive.Check(); !errors.Is(err, injectedErr) {
				t.Errorf("check should fail")
			}
		})
	}
}
