package main

import (
	"fmt"
	"log"

	"github.com/urfave/cli/v2"
)

var (
	dbDirectoryFlag = cli.StringFlag{
		Name:     "dir",
		Usage:    "the taregeted directory",
		Required: true,
	}
)

var getInfoCommand = cli.Command{
	Action: getInfo,
	Name:   "info",
	Usage:  "prints summary information about a state DB directory",
	Flags: []cli.Flag{
		&dbDirectoryFlag,
	},
}

func getInfo(ctx *cli.Context) (err error) {
	dir := ctx.String(dbDirectoryFlag.Name)
	log.Printf("Opening state in %v ...", dir)
	state, err := open(dir)
	if err != nil {
		return err
	}
	defer func() {
		log.Printf("Closing state in %v ...", dir)
		if closeError := state.Close(); closeError != nil {
			if err == nil {
				err = closeError
			} else {
				log.Printf("Failure closing DB: %v", closeError)
			}
		}
	}()

	log.Printf("Computing state hash ...")
	hash, err := state.GetHash()
	if err != nil {
		return
	}
	fmt.Printf("State hash: %v\n", hash)

	return nil
}
