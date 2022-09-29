package common

var (
	// BalanceKey is a respective "table space"
	BalanceKey = []byte("B")
	// NonceKey is a respective "table space"
	NonceKey = []byte("N")
	// SlotKey is a respective "table space"
	SlotKey = []byte("S")
	// ValueKey is a respective "table space"
	ValueKey = []byte("V")
)

// AppendKey converts the input key to its respective table space
func AppendKey(prefix, key []byte) []byte {
	return append(prefix, key...)
}

// AppendKeyStr converts the input key to its respective table space
func AppendKeyStr(prefix []byte, key string) []byte {
	return append(prefix, key...)
}
