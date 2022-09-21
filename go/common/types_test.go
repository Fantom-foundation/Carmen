package common_test

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

func TestStringSerializableImplements(t *testing.T) {
	var str common.StringSerializable
	var _ common.Serializable = str
}
