package demo

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend"
)

func TestRemoteSnapshotData_IsSnapshoutData(t *testing.T) {
	var _ backend.SnapshotData = &RemoteSnapshotData{}
}
