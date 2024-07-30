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
	"encoding/json"
	"os"
)

// ReadJsonFile reads a JSON file and unmarshals it into a struct of type T.
func ReadJsonFile[T any](file string) (T, error) {
	var zero T
	data, err := os.ReadFile(file)
	if err != nil {
		return zero, err
	}
	var res T
	if err := json.Unmarshal(data, &res); err != nil {
		return zero, err
	}
	return res, nil
}

// WriteJsonFile marshals a struct of type T into a JSON file.
func WriteJsonFile[T any](file string, data T) error {
	content, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return os.WriteFile(file, content, 0600)
}
