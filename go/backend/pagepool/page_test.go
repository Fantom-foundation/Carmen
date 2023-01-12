package pagepool

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func TestSortedPageIsMap(t *testing.T) {
	var instance Page[common.Address, uint32]
	var _ common.MultiMap[common.Address, uint32] = &instance
}
