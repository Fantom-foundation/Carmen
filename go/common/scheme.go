package common

// TableSpace divide key-value storage into spaces by adding a prefix to the key.
type TableSpace byte

const (
	// AccountStoreKey is a tablespace for accounts states
	AccountStoreKey TableSpace = 'C'
	// BalanceStoreKey is a tablespace for balances
	BalanceStoreKey TableSpace = 'B'
	// NonceStoreKey is a tablespace for nonces
	NonceStoreKey TableSpace = 'N'
	// ValueStoreKey is a tablespace for slot values
	ValueStoreKey TableSpace = 'V'
	// HashKey is a sub-tablespace for a hash tree
	HashKey TableSpace = 'H'
	// AddressIndexKey is a tablespace for address index
	AddressIndexKey TableSpace = 'A'
	// SlotLocIndexKey is a tablespace for slot index
	SlotLocIndexKey TableSpace = 'L'
	// KeyIndexKey is a tablespace for key index
	KeyIndexKey TableSpace = 'K'
	// DepotCodeKey is a tablespace for code depot
	DepotCodeKey TableSpace = 'D'
	// CodeHashStoreKey is a tablespace for store of codes hashes
	CodeHashStoreKey TableSpace = 'c'
	// AddressSlotMultiMapKey is a tablespace for slots-used-by-address multimap
	AddressSlotMultiMapKey TableSpace = 'M'
	// ReincarnationStoreKey is a tablespace for accounts reincarnations counters
	ReincarnationStoreKey TableSpace = 'R'

	// BlockArchiveKey is a tablespace for archive mapping from block numbers to block hashes
	BlockArchiveKey TableSpace = '1'
	// AccountArchiveKey is a tablespace for archive account states
	AccountArchiveKey TableSpace = '2'
	// BalanceArchiveKey is a tablespace for archive balances
	BalanceArchiveKey TableSpace = '3'
	// CodeArchiveKey is a tablespace for archive codes of contracts
	CodeArchiveKey TableSpace = '4'
	// NonceArchiveKey is a tablespace for archive nonces
	NonceArchiveKey TableSpace = '5'
	// StorageArchiveKey is a tablespace for storage slots values
	StorageArchiveKey TableSpace = '6'
	// AccountHashArchiveKey is a tablespace for archive account hashes
	AccountHashArchiveKey TableSpace = '7'
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
