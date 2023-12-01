package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

// Run using
//  go run ./state/mpt/tool <command> <flags>

func main() {
	app := &cli.App{
		Name:      "tool",
		Usage:     "Carmen MPT toolbox",
		Copyright: "(c) 2022-23 Fantom Foundation",
		Flags:     []cli.Flag{},
		Commands: []*cli.Command{
			&Check,
			&ExportCmd,
			&ImportCmd,
			&Info,
			&InitArchive,
			&Verify,
			&Benchmark,
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
