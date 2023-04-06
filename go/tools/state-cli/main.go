package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

// Run with `go run ./tools/state-cli`

func main() {
	app := &cli.App{
		Name:      "Carmen State Toolbox",
		HelpName:  "state",
		Usage:     "A set of utilities to inspect state DB directories",
		Copyright: "(c) 2023 Fantom Foundation",
		Flags:     []cli.Flag{},
		Commands: []*cli.Command{
			&getInfoCommand,
			&syncCommand,
		},
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
