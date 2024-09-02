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
	"errors"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/backend/utils"
	"go.uber.org/mock/gomock"
	"os"
	"path/filepath"
	"testing"
)

func TestReadOnlyFile_Get(t *testing.T) {
	const Items = 3300
	directory := t.TempDir()
	s, err := initFileStock(directory, Items)
	if err != nil {
		t.Fatalf("cannot init stock: %s", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Fatalf("cannot close stock: %s", err)
		}
	}()

	if err := s.Flush(); err != nil {
		t.Fatalf("cannot flush stock: %s", err)
	}

	readonly, err := OpenReadOnlyStock[int, int](directory, stock.IntEncoder{})
	if err != nil {
		t.Fatalf("cannot open stock: %s", err)
	}

	for i := 0; i < Items; i++ {
		got, err := readonly.Get(i)
		if err != nil {
			t.Fatalf("cannot get value: %s", err)
		}
		want, err := s.Get(i)
		if err != nil {
			t.Fatalf("cannot get value: %s", err)
		}

		if got != want {
			t.Errorf("value mismatch: got %d, want %d", got, want)
		}
	}
}

func TestReadOnlyFile_Open_MissingFile(t *testing.T) {
	directory := t.TempDir()
	s, err := initFileStock(directory, 10)
	if err != nil {
		t.Fatalf("cannot init stock: %s", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("cannot close stock: %s", err)
	}

	// delete file
	if err := os.Remove(filepath.Join(directory, fileNameValues)); err != nil {
		t.Fatalf("cannot delete file: %s", err)
	}

	if _, err := OpenReadOnlyStock[int, int](directory, stock.IntEncoder{}); err == nil {
		t.Errorf("opening stock should fail")
	}

	if _, err := openReadOnlyStock[int, int](directory, stock.IntEncoder{}); err == nil {
		t.Errorf("opening stock should fail")
	}
}

func TestReadOnlyFile_Failing_IO(t *testing.T) {
	injectedErr := fmt.Errorf("injected error")

	t.Run("failing seek", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		file := utils.NewMockOsFile(ctrl)
		file.EXPECT().Seek(gomock.Any(), gomock.Any()).Return(int64(0), injectedErr)

		encoder := stock.NewMockValueEncoder[any](ctrl)
		encoder.EXPECT().GetEncodedSize().Return(1)

		s := readonly[int, any]{file: file, encoder: encoder}
		if _, err := s.Get(0); !errors.Is(err, injectedErr) {
			t.Errorf("expected error %v, got %v", injectedErr, err)
		}
	})

	t.Run("failing read", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		file := utils.NewMockOsFile(ctrl)
		file.EXPECT().Seek(gomock.Any(), gomock.Any()).Return(int64(0), nil)
		file.EXPECT().Read(gomock.Any()).Return(0, injectedErr)

		encoder := stock.NewMockValueEncoder[any](ctrl)
		encoder.EXPECT().GetEncodedSize().Return(1)

		s := readonly[int, any]{file: file, encoder: encoder}
		if _, err := s.Get(0); !errors.Is(err, injectedErr) {
			t.Errorf("expected error %v, got %v", injectedErr, err)
		}
	})

	t.Run("failing close", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		file := utils.NewMockOsFile(ctrl)
		file.EXPECT().Close().Return(injectedErr)

		encoder := stock.NewMockValueEncoder[any](ctrl)

		s := readonly[int, any]{file: file, encoder: encoder}
		if err := s.Close(); !errors.Is(err, injectedErr) {
			t.Errorf("expected error %v, got %v", injectedErr, err)
		}
	})

	t.Run("failing encoder", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		file := utils.NewMockOsFile(ctrl)
		file.EXPECT().Seek(gomock.Any(), gomock.Any()).Return(int64(0), nil)
		file.EXPECT().Read(gomock.Any()).Return(0, nil)

		encoder := stock.NewMockValueEncoder[any](ctrl)
		encoder.EXPECT().GetEncodedSize().Return(1)
		encoder.EXPECT().Load(gomock.Any(), gomock.Any()).Return(injectedErr)

		s := readonly[int, any]{file: file, encoder: encoder}
		if _, err := s.Get(0); !errors.Is(err, injectedErr) {
			t.Errorf("expected error %v, got %v", injectedErr, err)
		}
	})
}
