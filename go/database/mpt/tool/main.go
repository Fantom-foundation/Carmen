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
	"log"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strings"

	"github.com/urfave/cli/v2"
)

// Run using
//  go run ./database/mpt/tool <command> <flags>

var (
	diagnosticsFlag = cli.IntFlag{
		Name:  "diagnostic-port",
		Usage: "enable hosting of a realtime diagnostic server by providing a port",
		Value: 0,
	}
	cpuProfileFlag = cli.StringFlag{
		Name:  "cpuprofile",
		Usage: "sets the target file for storing CPU profiles to, disabled if empty",
		Value: "",
	}
	traceFlag = cli.StringFlag{
		Name:  "tracefile",
		Usage: "sets the target file for traces to, disabled if empty",
		Value: "",
	}
)

func main() {
	app := &cli.App{
		Name:      "tool",
		Usage:     "Carmen MPT toolbox",
		Copyright: "(c) 2022-24 Fantom Foundation",
		Flags: []cli.Flag{
			&diagnosticsFlag,
			&cpuProfileFlag,
			&traceFlag,
		},
		Commands: []*cli.Command{
			&Check,
			&ExportCmd,
			&ImportLiveDbCmd,
			&ImportArchiveCmd,
			&ImportLiveAndArchiveCmd,
			&Info,
			&InitArchive,
			&Verify,
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

func addPerformanceDiagnoses(action cli.ActionFunc) cli.ActionFunc {
	return func(context *cli.Context) error {

		// Start the diagnostic service if requested.
		diagnosticPort := context.Int(diagnosticsFlag.Name)
		startDiagnosticServer(diagnosticPort)

		// Start CPU profiling.
		cpuProfileFileName := context.String(cpuProfileFlag.Name)
		if strings.TrimSpace(cpuProfileFileName) != "" {
			if err := startCpuProfiler(cpuProfileFileName); err != nil {
				return err
			}
			defer stopCpuProfiler()
		}

		// Start recording a trace.
		traceFileName := context.String(traceFlag.Name)
		if strings.TrimSpace(traceFileName) != "" {
			if err := startTracer(traceFileName); err != nil {
				return err
			}
			defer stopTracer()
		}

		return action(context)
	}
}

func startDiagnosticServer(port int) {
	if port <= 0 || port >= (1<<16) {
		return
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

func startCpuProfiler(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("could not create CPU profile: %s", err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		return fmt.Errorf("could not start CPU profile: %s", err)
	}
	return nil
}

func stopCpuProfiler() {
	pprof.StopCPUProfile()
}

func startTracer(filename string) error {
	traceFile, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create trace file: %v", err)
	}
	if err := trace.Start(traceFile); err != nil {
		return fmt.Errorf("failed to start trace: %v", err)
	}
	return nil
}

func stopTracer() {
	trace.Stop()
}
