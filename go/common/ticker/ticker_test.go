// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package ticker

import (
	"testing"
	"time"
)

func TestTicker_Ticks(t *testing.T) {
	last := time.Now()

	ticker := NewTimeTicker(10 * time.Millisecond)

	const loops = 100
	for i := 0; i < loops; i++ {
		tick := <-ticker.C()
		if tick.Before(last) {
			t.Errorf("tick %d is before last tick: %v < %v", i, last, tick)
		}
		last = tick
	}
	ticker.Stop()

	// no more ticks should be received
	select {
	case tick := <-ticker.C():
		t.Errorf("unexpected tick: %v", tick)
	case <-time.After(1000 * time.Millisecond):
		// done
	}
}
