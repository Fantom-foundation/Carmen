package common

// Log summarizes a log message recorded during the execution of a contract.
// This should be approximating ethereum's definition: t.ly/dVL7
type Log struct {
	// -- payload --
	// Address of the contract that generated the event.
	Address Address
	// List of topics the log message should be tagged by.
	Topics []Hash
	// The actual log message.
	Data []byte

	// -- metadata --
	// Index of the log in the block.
	Index uint
}
