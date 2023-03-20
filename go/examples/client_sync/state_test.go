package demo

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend"
)

func TestDemoState_IsSnapshotable(t *testing.T) {
	var _ backend.Snapshotable = &DemoState{}
}
