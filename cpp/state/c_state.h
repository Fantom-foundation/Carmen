// This header file defines a C interface for manipulating the world state.
// It is intendend to be used to bridge the Go/C++ boundary.

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
// increase readability, not to enforce any type contraints.

#define C_State void*

#define C_Address void*
#define C_Key void*
#define C_Value void*
#define C_Balance void*
#define C_Nonce void*

#define C_Hash void*

// ------------------------------ Life Cycle ----------------------------------

// Create a new state object and returns an opaque pointer to it. Ownership of
// the state is transfered to the caller, which is required to release it
// eventually.
C_State Carmen_CreateState();

// Releases a state object, thereby causing its destruction. After releasing it,
// no more operations may be applied on it.
void Carmen_ReleaseState(C_State state);

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

// ------------------------------ Global Hash ---------------------------------

// Retrieves a global state hash of the given state.
void Carmen_GetHash(C_State state, C_Hash out_hash);

#if __cplusplus
}
#endif
