package common

// TableSpace divide key-value storage into spaces by adding a prefix to the key.
type TableSpace byte

const (
	// BalanceKey is a respective "table space"
	BalanceKey TableSpace = 'B'
	// NonceKey is a respective "table space"
	NonceKey TableSpace = 'N'
	// SlotKey is a respective "table space"
	SlotKey TableSpace = 'S'
	// ValueKey is a respective "table space"
	ValueKey TableSpace = 'V'
	// HashKey is a respective "table space" for the hash tree
	HashKey TableSpace = 'H'
)

// AppendKey converts the input key to its respective table space
func (t TableSpace) AppendKey(key []byte) []byte {
	b := []byte{byte(t)}
	return append(b, key...)
}

// AppendKeyStr converts the input key to its respective table space
func (t TableSpace) AppendKeyStr(key string) []byte {
	b := []byte{byte(t)}
	return append(b, key...)
}
