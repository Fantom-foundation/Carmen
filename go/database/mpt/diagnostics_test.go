// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package mpt

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
)

func init() {
	// This init function starts a diagnostic server running in parallel to the unit tests
	// of this package. The main intention of the server is to facilitate the diagnoses of
	// performance issues and memory leaks tests. The server is started only if the
	// environment variable MPT_DIAGNOSTIC_PORT is set to a valid port number.
	//
	// Example:
	// $ MPT_DIAGNOSTIC_PORT=6060 go test -v ./database/mpt
	//
	portSpec := os.Getenv("MPT_DIAGNOSTIC_PORT")
	if portSpec == "" {
		return
	}

	port, err := strconv.Atoi(portSpec)
	if err != nil {
		fmt.Printf("invalid diagnostic port: %s\n", portSpec)
		os.Exit(1)
	}

	fmt.Printf("Starting diagnostic server at port http://localhost:%d\n", port)
	fmt.Printf("(see https://pkg.go.dev/net/http/pprof#hdr-Usage_examples for usage examples)\n")
	fmt.Printf("Block and mutex sampling rate is set to 100%% for diagnostics, which may impact overall performance\n")
	go func() {
		addr := fmt.Sprintf("localhost:%d", port)
		log.Println(http.ListenAndServe(addr, nil))
	}()
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)
}
