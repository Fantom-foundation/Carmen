package common

// TableSpace divide key-value storage into spaces by adding a prefix to the key.
type TableSpace byte

const (
	// BalanceKey is a respective "table space"
	BalanceKey TableSpace = 'B'
	// NonceKey is a respective "table space"
	NonceKey = 'N'
	// SlotKey is a respective "table space"
	SlotKey = 'S'
	// ValueKey is a respective "table space"
	ValueKey = 'V'
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
