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
		Name:      "Carmen MPT Toolbox",
		Copyright: "(c) 2022-23 Fantom Foundation",
		Flags:     []cli.Flag{},
		Commands: []*cli.Command{
			&Info,
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
