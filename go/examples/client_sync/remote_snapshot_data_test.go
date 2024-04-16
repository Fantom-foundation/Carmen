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

package demo

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend"
)

func TestRemoteSnapshotData_IsSnapshoutData(t *testing.T) {
	var _ backend.SnapshotData = &RemoteSnapshotData{}
}
