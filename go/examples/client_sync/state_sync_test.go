//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public License v3.
//

package demo

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

type blockGenerator struct {
	nextBlock uint64
}

func (g *blockGenerator) GetNextBlockUpdateMessage() Message {
	blockNumber := g.nextBlock
	g.nextBlock++

	a := byte(g.nextBlock)
	b := byte(g.nextBlock >> 8)
	c := byte(g.nextBlock >> 16)
	d := byte(g.nextBlock >> 24)

	update := common.Update{}
	update.AppendCreateAccount(common.Address{a, b, c, d})
	update.AppendBalanceUpdate(common.Address{a, b, c, d}, common.Balance{12})
	update.AppendNonceUpdate(common.Address{a, b, c, d}, common.Nonce{14})
	update.AppendCodeUpdate(common.Address{a, b, c, d}, []byte{a, b, c})
	update.AppendSlotUpdate(common.Address{a, b, c, d}, common.Key{a, b}, common.Value{c, d})
	update.AppendSlotUpdate(common.Address{a, b, c, d}, common.Key{a, b + 1}, common.Value{c, d})

	return BlockUpdateBroadcast{
		block:  blockNumber,
		update: update,
	}
}

func TestStateSynchronization(t *testing.T) {
	const blocksPerIncrement = 1000

	blockGen := blockGenerator{}

	// We start by creating a network.
	var net Network = &DemoNetwork{}

	// Let's have our first client join the network.
	client1 := DemoClient{}
	if err := client1.Join(t, net); err != nil {
		t.Errorf("client 1 failed to join: %v", err)
	}

	// Run a few blocks and create a new snapshot.
	for i := 0; i < blocksPerIncrement; i++ {
		net.Broadcast(blockGen.GetNextBlockUpdateMessage())
	}
	net.Broadcast(EndOfEpochBroadcast{})

	// Now have another client join the network.
	client2 := DemoClient{}
	if err := client2.Join(t, net); err != nil {
		t.Errorf("client 2 failed to join: %v", err)
	}

	// Now, the state of client 1 and 2 should be in sync.
	if !client1.GetStateProof().Equal(client2.GetStateProof()) {
		t.Errorf("clients not in sync")
	}

	// Run a few more blocks and create a new snapshot.
	for i := 0; i < blocksPerIncrement; i++ {
		net.Broadcast(blockGen.GetNextBlockUpdateMessage())
	}
	net.Broadcast(EndOfEpochBroadcast{})

	client3 := DemoClient{}
	if err := client3.Join(t, net); err != nil {
		t.Errorf("client 3 failed to join: %v", err)
	}

	// Now, the state of client 1, 2 and 3 should be in sync.
	if !client1.GetStateProof().Equal(client2.GetStateProof()) {
		t.Errorf("clients not in sync")
	}
	if !client1.GetStateProof().Equal(client3.GetStateProof()) {
		t.Errorf("clients not in sync")
	}
}
