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

// Copyright 2017 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_PORT_PORT_CONFIG_H_
#define STORAGE_LEVELDB_PORT_PORT_CONFIG_H_

// Define to 1 if you have a definition for fdatasync() in <unistd.h>.
#define HAVE_FUNC_FDATASYNC 1

// Define to 1 if you have Google CRC32C.
#define HAVE_CRC32C 1

// Define to 1 if you have Google Snappy.
#define HAVE_SNAPPY 1

// Define to 1 if your processor stores words with the most significant byte
// first (like Motorola and SPARC, unlike Intel and VAX).
#define LEVELDB_IS_BIG_ENDIAN 0

#endif  // STORAGE_LEVELDB_PORT_PORT_CONFIG_H_
