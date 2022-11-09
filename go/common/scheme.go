package common

// TableSpace divide key-value storage into spaces by adding a prefix to the key.
type TableSpace byte

const (
	// AccountStoreKey is a respective "table space"
	AccountStoreKey TableSpace = 'C'
	// BalanceStoreKey is a respective "table space"
	BalanceStoreKey TableSpace = 'B'
	// NonceStoreKey is a respective "table space"
	NonceStoreKey TableSpace = 'N'
	// SlotStoreKey is a respective "table space"
	SlotStoreKey TableSpace = 'S'
	// ValueStoreKey is a respective "table space"
	ValueStoreKey TableSpace = 'V'
	// HashKey is a respective "table space" for the hash tree
	HashKey TableSpace = 'H'
	// AddressIndexKey is a respective "table space"
	AddressIndexKey TableSpace = 'A'
	// SlotLocIndexKey is a respective "table space"
	SlotLocIndexKey TableSpace = 'L'
	// KeyIndexKey is a respective "table space"
	KeyIndexKey TableSpace = 'K'
	// DepotCodeKey is a respective "table space"
	DepotCodeKey TableSpace = 'D'
	// CodeHashStoreKey is a respective "table space" for code hashes store
	CodeHashStoreKey TableSpace = 'c'
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
