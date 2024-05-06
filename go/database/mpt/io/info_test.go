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
	"os"
	"path"
	"testing"
)

func TestIO_CheckMptDirectoryAndGetInfo_DoesNotExist(t *testing.T) {
	if _, err := CheckMptDirectoryAndGetInfo("abc"); err == nil {
		t.Errorf("getting directory info should fail")
	}
}

func TestIO_CheckMptDirectoryAndGetInfo_Not_Dir(t *testing.T) {
	dir := t.TempDir()
	file := path.Join(dir, "file.txt")
	if _, err := os.Create(file); err != nil {
		t.Fatalf("cannot init file: %s", err)
	}
	if _, err := CheckMptDirectoryAndGetInfo(file); err == nil {
		t.Errorf("getting directory info should fail")
	}
}

func TestIO_CheckMptDirectoryAndGetInfo_MissingMeta(t *testing.T) {
	dir := t.TempDir()
	if _, err := CheckMptDirectoryAndGetInfo(dir); err == nil {
		t.Errorf("getting directory info should fail")
	}
}

func TestIO_CheckMptDirectoryAndGetInfo_CannotParseMeta(t *testing.T) {
	dir := t.TempDir()
	file := path.Join(dir, "forest.json")
	if err := os.Mkdir(file, os.FileMode(06400)); err != nil {
		t.Fatalf("cannot init file: %s", err)
	}
	if _, err := CheckMptDirectoryAndGetInfo(dir); err == nil {
		t.Errorf("getting directory info should fail")
	}
}

func TestIO_CheckMptDirectoryAndGetInfo_Unknown_Config(t *testing.T) {
	dir := t.TempDir()
	file := path.Join(dir, "forest.json")

	meta := "{\"Configuration\":\"S150-Dead\",\"Mutable\":true}"
	if err := os.WriteFile(file, []byte(meta), 0644); err != nil {
		t.Fatalf("cannot prepare for test: %s", err)
	}

	if _, err := CheckMptDirectoryAndGetInfo(dir); err == nil {
		t.Errorf("getting directory info should fail")
	}
}
