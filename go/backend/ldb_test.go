package backend

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"testing"
)

var dbKeySink DbKey

func BenchmarkConvertTableSpaceSerializer(b *testing.B) {
	serializer := common.KeySerializer{}
	prefix := common.BalanceStoreKey
	key := common.Key{}
	for i := 1; i <= b.N; i++ {
		key[0] = byte(i)
		bytes := serializer.ToBytes(key) // convert within the benchmark
		dbKeySink = ToDBKey(prefix, bytes)
	}
}
