// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

// Run using
//  go run ./database/mpt/tool <command> <flags>

func main() {
	app := &cli.App{
		Name:      "tool",
		Usage:     "Carmen MPT toolbox",
		Copyright: "(c) 2022-23 Fantom Foundation",
		Flags:     []cli.Flag{},
		Commands: []*cli.Command{
			&Check,
			&ExportCmd,
			&ImportLiveDbCmd,
			&ImportArchiveCmd,
			&ImportLiveAndArchiveCmd,
			&Info,
			&InitArchive,
			&Verify,
			&VerifyProof,
			&Benchmark,
			&Block,
			&StressTestCmd,
			&Reset,
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
