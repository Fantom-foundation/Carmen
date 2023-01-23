// This header file defines a C interface for manipulating the world state.
// It is intended to be used to bridge the Go/C++ boundary.

#include <stdint.h>

#if __cplusplus
extern "C" {
#endif

// The C interface for the storage system is designed to minimize overhead
// between Go and C. All data is passed as pointers and the memory management
// responsibility is generally left to the Go side. Parameters may serve as in
// or out parameters. Future extensions may utilize the return value as an error
// indicator.

// The following macro definitions provide syntactic sugar for type-erased
// pointers used in the interface definitions below. Their main purpose is to
// increase readability, not to enforce any type constraints.

#define C_State void*

#define C_Address void*
#define C_Key void*
#define C_Value void*
#define C_Balance void*
#define C_Nonce void*
#define C_Code void*

#define C_Hash void*
#define C_AccountState void*

// ------------------------------ Life Cycle ----------------------------------

// Creates a new state retaining all data in memory and returns an opaque
// pointer to it. Ownership of the state is transfered to the caller, which is
// required to release it eventually.
C_State Carmen_CreateInMemoryState();

// Creates a new state object maintaining data in files of the given directory
// and returns an opaque pointer to it. Ownership of the state is transfered to
// the caller, which is required to release it eventually.
C_State Carmen_CreateFileBasedState(const char* directory, int length);

// Creates a new state object maintaining data in a LevelDB instance located in
// the given directory and returns an opaque pointer to it. Ownership of the
// state is transferred to the caller, which is required to release it
// eventually.
C_State Carmen_CreateLevelDbBasedState(const char* directory, int length);

// Flushes all committed state information to disk to guarantee permanent
// storage. All internally cached modifications is synced to disk.
void Carmen_Flush(C_State state);

// Closes this state, releasing all IO handles and locks on external resources.
void Carmen_Close(C_State state);

// Releases a state object, thereby causing its destruction. After releasing it,
// no more operations may be applied on it.
void Carmen_ReleaseState(C_State state);

// ------------------------------- Accounts -----------------------------------

// Creates a new account or resurrects a deleted account.
void Carmen_CreateAccount(C_State state, C_Address addr);

// Gets the current state of the given account.
void Carmen_GetAccountState(C_State state, C_Address addr,
                            C_AccountState out_state);

// Deletes the given account.
void Carmen_DeleteAccount(C_State state, C_Address addr);

// -------------------------------- Balance -----------------------------------

// Retrieves the balance of the given account.
void Carmen_GetBalance(C_State state, C_Address addr, C_Balance out_balance);

// Updates the balance of the given account.
void Carmen_SetBalance(C_State state, C_Address addr, C_Balance balance);

// --------------------------------- Nonce ------------------------------------

// Retrieves the nonce of the given account.
void Carmen_GetNonce(C_State state, C_Address addr, C_Nonce out_nonce);

// Updates the nonce of the given account.
void Carmen_SetNonce(C_State state, C_Address addr, C_Nonce nonce);

// -------------------------------- Storage -----------------------------------

// Retrieves the value of storage location (addr,key) in the given state.
void Carmen_GetStorageValue(C_State state, C_Address addr, C_Key key,
                            C_Value out_value);

// Updates the value of storage location (addr,key) in the given state.
void Carmen_SetStorageValue(C_State state, C_Address addr, C_Key key,
                            C_Value value);

// --------------------------------- Code -------------------------------------

// Retrieves the code stored under the given address.
void Carmen_GetCode(C_State state, C_Address addr, C_Code out_code,
                    uint32_t* out_length);

// Updates the code stored under the given address.
void Carmen_SetCode(C_State state, C_Address addr, C_Code code,
                    uint32_t length);

// Retrieves the hash of the code stored under the given address.
void Carmen_GetCodeHash(C_State state, C_Address addr, C_Hash out_hash);

// Retrieves the code length stored under the given address.
void Carmen_GetCodeSize(C_State state, C_Address addr, uint32_t* out_length);

// ------------------------------ Global Hash ---------------------------------

// Retrieves a global state hash of the given state.
void Carmen_GetHash(C_State state, C_Hash out_hash);

// --------------------------- Memory Footprint -------------------------------

// Retrieves a summary of the used memory. After the call the out variable will
// point to a buffer with a serialized summary that needs to be freed by the
// caller.
void Carmen_GetMemoryFootprint(C_State state, char** out, uint64_t* out_length);

#if __cplusplus
}
#endif
