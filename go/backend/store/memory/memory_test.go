package memory

import (
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func TestImplements(t *testing.T) {
	var s Memory[uint64, common.StringSerializable]
	var _ store.Store[uint64, common.StringSerializable] = s
}
