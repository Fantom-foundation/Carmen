package common

// TableSpaces divide key-value storage into spaces by adding a prefix to the key.
type TableSpaces byte

const (
	// BalanceKey is a respective "table space"
	BalanceKey TableSpaces = 'B'
	// NonceKey is a respective "table space"
	NonceKey = 'N'
	// SlotKey is a respective "table space"
	SlotKey = 'S'
	// ValueKey is a respective "table space"
	ValueKey = 'V'
)

// AppendKey converts the input key to its respective table space
func AppendKey(prefix TableSpaces, key []byte) []byte {
	b := []byte{byte(prefix)}
	return append(b, key...)
}

// AppendKeyStr converts the input key to its respective table space
func AppendKeyStr(prefix TableSpaces, key string) []byte {
	b := []byte{byte(prefix)}
	return append(b, key...)
}
