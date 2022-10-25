package common

// TableSpace divide key-value storage into spaces by adding a prefix to the key.
type TableSpace byte

const (
	// AddressKey is a respective "table space"
	AddressKey TableSpace = 'A'
	// KeyKey is a respective "table space"
	KeyKey TableSpace = 'K'
	// AccountKey is a respective "table space"
	AccountKey TableSpace = 'C'
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

// DbKey expects max size of the 32B key plus at most two bytes
// for the table prefix (e.g. balance, nonce, slot, ...) and the domain (e.g. data, hash, ...)
type DbKey [34]byte

func (d DbKey) ToBytes() []byte {
	return d[:]
}

// ToDBKey converts the input key to its respective table space key
func (t TableSpace) ToDBKey(key []byte) DbKey {
	var dbKey DbKey
	dbKey[0] = byte(t)
	copy(dbKey[1:], key)
	return dbKey
}

// DBToDBKey converts the input key to its respective table space key
func (t TableSpace) DBToDBKey(key DbKey) DbKey {
	var dbKey DbKey
	dbKey[0] = byte(t)
	copy(dbKey[1:], key[:])
	return dbKey
}

// StrToDBKey converts the input key to its respective table space key
func (t TableSpace) StrToDBKey(key string) DbKey {
	var dbKey DbKey
	dbKey[0] = byte(t)
	copy(dbKey[1:], key)
	return dbKey
}
