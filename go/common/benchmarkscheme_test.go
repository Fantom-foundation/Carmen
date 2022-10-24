package common

import "testing"

var dbKeySink DbKey

func BenchmarkConvertTableSpaceSerializer(b *testing.B) {
	serializer := KeySerializer{}
	prefix := BalanceKey
	key := Key{}
	for i := 1; i <= b.N; i++ {
		key[0] = byte(i)
		bytes := serializer.ToBytes(key) // convert within the benchmark
		dbKeySink = prefix.ToDBKey(bytes)
	}
}
