/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public Licence v3.
 */

#ifndef THIRD_PARTY_SNAPPY_OPENSOURCE_CMAKE_CONFIG_H_
#define THIRD_PARTY_SNAPPY_OPENSOURCE_CMAKE_CONFIG_H_

/* Define to 1 if the compiler supports __builtin_ctz and friends. */
#define HAVE_BUILTIN_CTZ 1

/* Define to 1 if the compiler supports __builtin_expect. */
#define HAVE_BUILTIN_EXPECT 1

/* Define to 1 if you have the <byteswap.h> header file. */
#define HAVE_BYTESWAP_H 1

/* Define to 1 if you have a definition for mmap() in <sys/mman.h>. */
#define HAVE_FUNC_MMAP 1

/* Define to 1 if you have a definition for sysconf() in <unistd.h>. */
#define HAVE_FUNC_SYSCONF 1

/* Define to 1 to use the gflags package for command-line parsing. */
#define HAVE_GFLAGS 1

/* Define to 1 if you have Google Test. */
#define HAVE_GTEST 1

/* Define to 1 if you have the `lzo2' library (-llzo2). */
/* #undef HAVE_LIBLZO2 */

/* Define to 1 if you have the `z' library (-lz). */
#define HAVE_LIBZ 1

/* Define to 1 if you have the <sys/endian.h> header file. */
/* #undef HAVE_SYS_ENDIAN_H */

/* Define to 1 if you have the <sys/mman.h> header file. */
#define HAVE_SYS_MMAN_H 1

/* Define to 1 if you have the <sys/resource.h> header file. */
#define HAVE_SYS_RESOURCE_H 1

/* Define to 1 if you have the <sys/time.h> header file. */
#define HAVE_SYS_TIME_H 1

/* Define to 1 if you have the <sys/uio.h> header file. */
#define HAVE_SYS_UIO_H 1

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* Define to 1 if you have the <windows.h> header file. */
/* #undef HAVE_WINDOWS_H */

/* Define to 1 if you target processors with SSSE3+ and have <tmmintrin.h>. */
#define SNAPPY_HAVE_SSSE3 0

/* Define to 1 if your processor stores words with the most significant byte
   first (like Motorola and SPARC, unlike Intel and VAX). */
/* #undef SNAPPY_IS_BIG_ENDIAN */

#endif  // THIRD_PARTY_SNAPPY_OPENSOURCE_CMAKE_CONFIG_H_
