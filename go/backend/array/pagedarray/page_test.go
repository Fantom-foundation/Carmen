//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3.
//

package pagedarray

import (
	"github.com/Fantom-foundation/Carmen/go/backend/pagepool"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func TestPageDirtyFlag(t *testing.T) {
	tempDir := t.TempDir() + "/file.dat"
	s, err := pagepool.NewFilePageStorage(tempDir, common.PageSize)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	page := initPage()

	if dirty := page.IsDirty(); !dirty {
		t.Errorf("new page should be dirty")
	}

	_ = s.Store(1, page)

	if dirty := page.IsDirty(); dirty {
		t.Errorf("persisted page should not be dirty")
	}

	page.Clear()

	if dirty := page.IsDirty(); !dirty {
		t.Errorf("cleared page should be dirty")
	}

	_ = s.Load(1, page)

	if dirty := page.IsDirty(); dirty {
		t.Errorf("freshly loaded page should not be dirty")
	}

	dump := make([]byte, page.Size())
	page.ToBytes(dump)

	restoredPage := initPage()
	restoredPage.FromBytes(dump)
	if dirty := restoredPage.IsDirty(); !dirty {
		t.Errorf("page should be dirty")
	}
}

func initPage() *Page {
	return NewPage(common.PageSize)
}
