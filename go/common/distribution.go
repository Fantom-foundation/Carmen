// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package common

import (
	"math/rand"
)

// distribution is a type of random probability distribution
type distribution int

const (
	Sequential  distribution = 0
	Uniform     distribution = 1
	Exponential distribution = 2
)

// Distribution wraps a Label of the distribution and a function to get a next value withing the given distribution
type Distribution struct {
	Label   string
	GetNext func() uint32
}

// GetDistributions return a set of distributions
func GetDistributions(size int) []Distribution {
	expRate := float64(10) / float64(size)
	it := 0
	return []Distribution{
		{
			Label: "Sequential",
			GetNext: func() uint32 {
				it = (it + 1) % size
				return uint32(it)
			},
		},
		{
			Label: "Uniform",
			GetNext: func() uint32 {
				return uint32(rand.Intn(size))
			},
		},
		{
			Label: "Exponential",
			GetNext: func() uint32 {
				return uint32(rand.ExpFloat64()/expRate) % uint32(size)
			},
		},
	}
}

// GetDistribution returns a distribution
func (d distribution) GetDistribution(size int) Distribution {
	return GetDistributions(size)[d]
}
