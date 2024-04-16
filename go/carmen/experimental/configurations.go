//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3 
//

package experimental

import (
	"github.com/Fantom-foundation/Carmen/go/carmen"
	"github.com/Fantom-foundation/Carmen/go/state"

	_ "github.com/Fantom-foundation/Carmen/go/state/cppstate"
	_ "github.com/Fantom-foundation/Carmen/go/state/gostate"
)

// GetDatabaseConfigurations returns a list of experimental database configurations
// which should not be used in productive settings but may be used for special
// purpose setups.
// WARNING: do not use those configurations without understanding the involved
// risks. Consult Carmen developers if you have questions.
func GetDatabaseConfigurations() []carmen.Configuration {
	// Register all configurations with a known factory.
	res := []carmen.Configuration{}
	for config := range state.GetAllRegisteredStateFactories() {
		res = append(res, carmen.Configuration{
			Variant: carmen.Variant(config.Variant),
			Schema:  carmen.Schema(config.Schema),
			Archive: carmen.Archive(config.Archive),
		})
	}
	return res
}

func init() {
	for _, config := range GetDatabaseConfigurations() {
		carmen.RegisterConfiguration(config)
	}
}
