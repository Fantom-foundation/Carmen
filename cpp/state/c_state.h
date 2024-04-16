/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3 
 */

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
#define C_Schema uint8_t

#define C_bool uint8_t
#define C_Address void*
#define C_Key void*
#define C_Value void*
#define C_Balance void*
#define C_Nonce void*
#define C_Code void*
#define C_Update void*
#define C_Hash void*
#define C_AccountState void*

// An enumeration of supported state implementations.
enum StateImpl { kState_Memory = 0, kState_File = 1, kState_LevelDb = 2 };

// An enumeration of supported archive implementations.
enum ArchiveImpl {
  kArchive_None = 0,
  kArchive_LevelDb = 1,
  kArchive_Sqlite = 2
};

// ------------------------------ Life Cycle ----------------------------------

// Opens a new state object based on the provided implementation maintaining
// its data in the given directory. If the directory does not exist, it is
// created. If it is empty, a new, empty state is initialized. If it contains
// state information, the information is loaded.
//
// The function returns an opaque pointer to a state object that can be used
// with the remaining functions in this file. Ownership is transfered to the
// caller, which is required for releasing it eventually using Carmen_Release().
// If for some reason the creation of the state instance failed, a nullptr is
// returned.
C_State Carmen_OpenState(C_Schema schema, enum StateImpl state,
                         enum ArchiveImpl archive, const char* directory,
                         int length);

// Flushes all committed state information to disk to guarantee permanent
// storage. All internally cached modifications is synced to disk.
void Carmen_Flush(C_State state);

// Closes this state, releasing all IO handles and locks on external resources.
void Carmen_Close(C_State state);

// Releases a state object, thereby causing its destruction. After releasing it,
// no more operations may be applied on it.
void Carmen_ReleaseState(C_State state);

// ----------------------------- Archive State --------------------------------

// Creates a  state snapshot reflecting the state at the given block height. The
// resulting state must be released and must not outlive the life time of the
// provided state.
C_State Carmen_GetArchiveState(C_State state, uint64_t block);

// ------------------------------- Accounts -----------------------------------

// Gets the current state of the given account.
void Carmen_GetAccountState(C_State state, C_Address addr,
                            C_AccountState out_state);

// -------------------------------- Balance -----------------------------------

// Retrieves the balance of the given account.
void Carmen_GetBalance(C_State state, C_Address addr, C_Balance out_balance);

// --------------------------------- Nonce ------------------------------------

// Retrieves the nonce of the given account.
void Carmen_GetNonce(C_State state, C_Address addr, C_Nonce out_nonce);

// -------------------------------- Storage -----------------------------------

// Retrieves the value of storage location (addr,key) in the given state.
void Carmen_GetStorageValue(C_State state, C_Address addr, C_Key key,
                            C_Value out_value);

// --------------------------------- Code -------------------------------------

// Retrieves the code stored under the given address.
void Carmen_GetCode(C_State state, C_Address addr, C_Code out_code,
                    uint32_t* out_length);

// Retrieves the hash of the code stored under the given address.
void Carmen_GetCodeHash(C_State state, C_Address addr, C_Hash out_hash);

// Retrieves the code length stored under the given address.
void Carmen_GetCodeSize(C_State state, C_Address addr, uint32_t* out_length);

// -------------------------------- Update ------------------------------------

// Applies the provided block update to the maintained state.
void Carmen_Apply(C_State state, uint64_t block, C_Update update,
                  uint64_t length);

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
