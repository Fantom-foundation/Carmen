package main

import (
	"fmt"
	"log"
	"time"

	"github.com/urfave/cli/v2"
)

var (
	cpuProfilingFlag = cli.StringFlag{
		Name:  "cpu-profile",
		Usage: "enable the recording of a CPU profile",
	}
	dbSourceDirFlag = cli.StringFlag{
		Name:     "src-dir",
		Usage:    "the source of the synchronization",
		Required: true,
	}
	dbTargetDirFlag = cli.StringFlag{
		Name:     "trg-dir",
		Usage:    "the target of the synchronization",
		Required: true,
	}
)

var syncCommand = cli.Command{
	Action: sync,
	Name:   "sync",
	Usage:  "syncs one state DB directory to contain the content of another",
	Flags: []cli.Flag{
		&dbSourceDirFlag,
		&dbTargetDirFlag,
		&cpuProfilingFlag,
	},
}

func sync(ctx *cli.Context) (err error) {

	profileTarget := ctx.String(cpuProfilingFlag.Name)
	if len(profileTarget) != 0 {
		if err := StartCPUProfile(profileTarget); err != nil {
			return err
		}
		defer StopCPUProfile()
	}

	srcDir := ctx.String(dbSourceDirFlag.Name)
	log.Printf("Opening source state in %v ...", srcDir)
	source, err := open(srcDir)
	if err != nil {
		return err
	}
	defer func() {
		log.Printf("Closing source state in %v ...", srcDir)
		if closeError := source.Close(); closeError != nil {
			if err == nil {
				err = closeError
			} else {
				log.Printf("Failure closing DB: %v", closeError)
			}
		}
	}()

	trgDir := ctx.String(dbTargetDirFlag.Name)
	log.Printf("Opening target state in %v ...", trgDir)
	target, err := open(trgDir)
	if err != nil {
		return err
	}
	defer func() {
		log.Printf("Closing target state in %v ...", trgDir)
		if closeError := target.Close(); closeError != nil {
			if err == nil {
				err = closeError
			} else {
				log.Printf("Failure closing DB: %v", closeError)
			}
		}
	}()

	sourceHash, err := source.GetHash()
	if err != nil {
		return
	}
	fmt.Printf("Source state hash: %v\n", sourceHash)

	targetHash, err := target.GetHash()
	if err != nil {
		return
	}
	fmt.Printf("Target state hash: %v\n", targetHash)

	log.Printf("Synching states ...")

	start := time.Now()
	snapshot, err := source.CreateSnapshot()
	if err != nil {
		return err
	}
	data := snapshot.GetData()

	if err = target.Restore(data); err != nil {
		return err
	}

	log.Printf("Synching complete")
	log.Printf("Synching took %.1f seconds", time.Since(start).Seconds())

	targetHash, err = target.GetHash()
	if err != nil {
		return
	}
	fmt.Printf("Target state hash: %v\n", targetHash)

	if sourceHash != targetHash {
		return fmt.Errorf("sync failed, hashes are not equivalen")
	}

	return nil
}
