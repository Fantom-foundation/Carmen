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
	"os"
	"path/filepath"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/backend/utils"
)

func TestCodes_OpenCodes(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "codes.dat")
	codes, err := openCodes(file, dir)
	if err != nil {
		t.Fatalf("failed to open codes: %v", err)
	}

	if want, got := 0, len(codes.codes); want != got {
		t.Fatalf("expected codes to be empty, got %d", got)
	}
}

func TestCodes_CodesCanBeAddedAndRetrieved(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "codes.dat")
	codes, err := openCodes(file, dir)
	if err != nil {
		t.Fatalf("failed to open codes: %v", err)
	}

	code1 := []byte("code1")
	code2 := []byte("code2")

	hash1 := codes.add(code1)
	hash2 := codes.add(code2)

	if want, got := 2, len(codes.codes); want != got {
		t.Fatalf("expected codes to have 2 entries, got %d", got)
	}

	if want, got := code1, codes.getCodeForHash(hash1); string(want) != string(got) {
		t.Fatalf("expected code1, got %s", got)
	}

	if want, got := code2, codes.getCodeForHash(hash2); string(want) != string(got) {
		t.Fatalf("expected code2, got %s", got)
	}
}

func TestCodes_CheckpointsCanBeRestored(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "codes.dat")
	codes, err := openCodes(file, dir)
	if err != nil {
		t.Fatalf("failed to open codes: %v", err)
	}

	codes.add([]byte("code1"))
	codes.add([]byte("code2"))

	checkpoint := utils.Checkpoint(1)
	if err := codes.Prepare(checkpoint); err != nil {
		t.Fatalf("failed to prepare checkpoint: %v", err)
	}

	if err := codes.Commit(checkpoint); err != nil {
		t.Fatalf("failed to commit checkpoint: %v", err)
	}

	backup, err := os.Stat(file)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	codes.add([]byte("code3"))
	if want, got := 3, len(codes.codes); want != got {
		t.Fatalf("expected codes to have %d entries, got %d", want, got)
	}

	if err := codes.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	modified, err := os.Stat(file)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	if modified.Size() <= backup.Size() {
		t.Fatalf("expected file to be larger after flush")
	}

	if err := codes.Restore(checkpoint); err != nil {
		t.Fatalf("failed to restore checkpoint: %v", err)
	}
	if want, got := 2, len(codes.codes); want != got {
		t.Fatalf("expected codes to have %d entries, got %d", want, got)
	}

	restored, err := os.Stat(file)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	if restored.Size() != backup.Size() {
		t.Fatalf("expected file to be same size after restore")
	}
}

func TestCodes_CheckpointsCanBeAborted(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "codes.dat")
	codes, err := openCodes(file, dir)
	if err != nil {
		t.Fatalf("failed to open codes: %v", err)
	}

	codes.add([]byte("code1"))
	codes.add([]byte("code2"))

	checkpoint := utils.Checkpoint(1)
	if err := codes.Prepare(checkpoint); err != nil {
		t.Fatalf("failed to prepare checkpoint: %v", err)
	}

	if err := codes.Abort(checkpoint); err != nil {
		t.Fatalf("failed to commit checkpoint: %v", err)
	}

	if want, got := 2, len(codes.codes); want != got {
		t.Fatalf("expected codes to have %d entries, got %d", want, got)
	}

	checkpoint = utils.Checkpoint(0)
	if err := codes.Restore(checkpoint); err != nil {
		t.Fatalf("failed to restore checkpoint: %v", err)
	}

	if want, got := 0, len(codes.codes); want != got {
		t.Fatalf("expected codes to have %d entries, got %d", want, got)
	}
}

func TestCodes_CanBeHandledByCheckpointCoordinator(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "codes.dat")
	codes, err := openCodes(file, dir)
	if err != nil {
		t.Fatalf("failed to open codes: %v", err)
	}

	coordinator, err := utils.NewCheckpointCoordinator(t.TempDir(), codes)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}

	codes.add([]byte("code1"))

	if _, err := coordinator.CreateCheckpoint(); err != nil {
		t.Fatalf("failed to create checkpoint: %v", err)
	}

	codes.add([]byte("code2"))

	if err := coordinator.Restore(); err != nil {
		t.Fatalf("failed to restore checkpoint: %v", err)
	}

	if want, got := 1, len(codes.codes); want != got {
		t.Fatalf("expected codes to have %d entries, got %d", want, got)
	}

}
