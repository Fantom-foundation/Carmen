// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package utils

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadJsonFile_CanReadJsonData(t *testing.T) {
	type Data struct {
		Name string
		Age  int
	}
	dir := t.TempDir()
	file := filepath.Join(dir, "data.json")
	if err := os.WriteFile(file, []byte(`{"Name":"John","Age":30}`), 0600); err != nil {
		t.Fatal(err)
	}

	data, err := ReadJsonFile[Data](file)
	if err != nil {
		t.Fatal(err)
	}
	if data.Name != "John" {
		t.Error("Name should be John")
	}
	if data.Age != 30 {
		t.Error("Age should be 30")
	}
}

func TestReadJsonFile_DetectsIoError(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "data.json")
	if err := os.WriteFile(file, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(file, 0); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(file, 0600)

	_, err := ReadJsonFile[chan bool](file)
	if err == nil {
		t.Error("Expected an error")
	}
}

func TestReadJsonFile_DetectsMarshalingError(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "data.json")
	if err := os.WriteFile(file, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}

	_, err := ReadJsonFile[chan bool](file)
	if err == nil {
		t.Error("Expected an error")
	}
}

func TestWriteJsonFile_CanWriteJsonData(t *testing.T) {
	type Data struct {
		Name string
		Age  int
	}
	dir := t.TempDir()
	file := filepath.Join(dir, "data.json")
	if err := WriteJsonFile(file, Data{Name: "John", Age: 30}); err != nil {
		t.Fatalf("failed to write JSON file: %v", err)
	}

	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("failed to read JSON file: %v", err)
	}

	if string(data) != `{"Name":"John","Age":30}` {
		t.Error("JSON data is incorrect")
	}
}

func TestWriteJsonFile_DetectsIoError(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "data.json")
	if err := os.WriteFile(file, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(file, 0); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(file, 0600)

	err := WriteJsonFile(file, "test")
	if err == nil {
		t.Error("Expected an error")
	}
}

func TestWriteJsonFile_DetectsMarshalingError(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "data.json")
	err := WriteJsonFile(file, make(chan bool))
	if err == nil {
		t.Error("Expected an error")
	}
}
