package backend

import (
	"github.com/Fantom-foundation/Carmen/go/common"
	"slices"
	"testing"
)

var dbKeySink DbKey

func TestDbKey_Too_Long(t *testing.T) {
	var dbKey DbKey
	key := make([]byte, len(dbKey))

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("ToDBKey should panic")
		}
	}()

	ToDBKey(BalanceArchiveKey, key)
}

func TestDbKey_ToDBKey(t *testing.T) {
	var dbKey DbKey
	key := make([]byte, len(dbKey)-1)
	for i := 0; i < len(dbKey)-1; i++ {
		key[i] = byte(i)
	}
	expected := append([]byte{byte(BalanceArchiveKey)}, key...)

	if got, want := ToDBKey(BalanceArchiveKey, key).ToBytes(), expected; !slices.Equal(got, want) {
		t.Errorf("keys do not match: %v != %v", got, want)
	}
}

func TestDbKey_ShortKey(t *testing.T) {
	var dbKey DbKey
	key := make([]byte, 5)
	expected := make([]byte, len(dbKey))
	expected[0] = byte(BalanceArchiveKey)

	for i := 0; i < len(key); i++ {
		key[i] = byte(i)
		expected[i+1] = key[i]
	}

	if got, want := ToDBKey(BalanceArchiveKey, key).ToBytes(), expected; !slices.Equal(got, want) {
		t.Errorf("keys do not match: %v != %v", got, want)
	}
}

func BenchmarkConvertTableSpaceSerializer(b *testing.B) {
	serializer := common.KeySerializer{}
	prefix := BalanceStoreKey
	key := common.Key{}
	for i := 1; i <= b.N; i++ {
		key[0] = byte(i)
		bytes := serializer.ToBytes(key) // convert within the benchmark
		dbKeySink = ToDBKey(prefix, bytes)
	}
}
