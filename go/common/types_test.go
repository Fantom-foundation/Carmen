package common_test

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
	"testing"
)

func TestImplements(t *testing.T) {
	var str state.Address
	var _ common.Serializable = str
}
