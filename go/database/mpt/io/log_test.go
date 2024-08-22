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
	"bytes"
	"log"
	"regexp"
	"testing"
	"time"
)

func TestLog_Print(t *testing.T) {
	var buf bytes.Buffer

	logger := Log{logger: log.New(&buf, "", 0), start: time.Now()}
	logger.Print("Test message")

	if got, want := buf.String(), regexp.MustCompile(`\[t=.*?] - Test message`); !want.MatchString(got) {
		t.Errorf("unexpected log content: got %q, want %q", got, want)
	}
}

func TestLog_Printf(t *testing.T) {
	var buf bytes.Buffer

	logger := Log{logger: log.New(&buf, "", 0), start: time.Now()}
	logger.Printf("Test message %d", 42)

	if got, want := buf.String(), regexp.MustCompile(`\[t=.*?] - Test message 42`); !want.MatchString(got) {
		t.Errorf("unexpected log content: got %q, want %q", got, want)
	}
}

func TestProgressLogger_Step(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLog()
	logger.logger = log.New(&buf, "", 0)

	progressLogger := logger.NewProgressTracker("Progress: %d steps, %.2f steps/sec", 10)

	progressLogger.Step(5)
	progressLogger.Step(3)
	progressLogger.Step(2)

	if got, want := buf.String(), regexp.MustCompile(`\[t=.*?] - Progress: 10 steps, \d+\.\d+ steps/sec`); !want.MatchString(got) {
		t.Errorf("unexpected log content: got %q, want %q", got, want)
	}
}
