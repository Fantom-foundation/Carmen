// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package io

import (
	"fmt"
	"log"
	"time"
)

// Log is a logger customised for the MPT tool output.
// In particular, it prints the time elapsed since the start of the program.
type Log struct {
	start  time.Time
	logger *log.Logger
}

// NewLog creates a new logger.
func NewLog() *Log {
	return &Log{start: time.Now(), logger: log.Default()}
}

// Print logs a message that includes the time elapsed since the start of the program.
func (l *Log) Print(msg string) {
	now := time.Now()
	t := uint64(now.Sub(l.start).Seconds())
	l.logger.Printf("[t=%4d:%02d] - %s\n", t/60, t%60, msg)
}

// Printf logs a formatted message that includes the time elapsed since the start of the program.
func (l *Log) Printf(format string, v ...any) {
	l.Print(fmt.Sprintf(format, v...))
}

// ProgressLogger is a logger that tracks the progress of a task.
// It logs the progress at regular intervals configured when creating this logger.
type ProgressLogger struct {
	log            *Log
	start          time.Time
	format         string
	window         int
	counter, steps int
}

// NewProgressTracker creates a new ProgressLogger.
func (l *Log) NewProgressTracker(format string, window int) *ProgressLogger {
	return &ProgressLogger{log: l, start: time.Now(), format: format, window: window}
}

// Step increments the progress counter by the given number of steps.
// If the counter reaches the window size, the progress is logged.
func (p *ProgressLogger) Step(increment int) {
	p.counter += increment
	p.steps += increment

	if p.steps >= p.window {
		now := time.Now()

		count := p.counter / p.window * p.window // round down to the nearest window size
		p.log.Printf(p.format, count, float64(p.steps)/now.Sub(p.start).Seconds())

		p.steps = 0
		p.start = now
	}
}

// GetCounter returns the current value of the progress counter.
func (p *ProgressLogger) GetCounter() int {
	return p.counter
}
