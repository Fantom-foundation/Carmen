// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package demo

// Address is used like an IP address in the demo network.
type Address int

// Network is simulating the internet, allowing clients to join and leave, and
// only exchange messages in a serialized form. In particular, it is not
// possible to get a reference to another client.
type Network interface {
	// Register a new client to this network and return its address.
	Register(Client) Address

	// Unregister a client from the network.
	Unregister(Client)

	// GetAllAddresses retrieves all addresses of currently active nodes.
	GetAllAddresses() []Address

	// Call sends the message to the given address and waits for its response.
	// The result is nil if the client is not available.
	Call(Address, Message) Message

	// Broadcast distribute the given message to all clients in the network.
	Broadcast(Message)
}

// DemoNetwork is a in-memory, single process implementation of a network.
type DemoNetwork struct {
	clients []Client
}

func (n *DemoNetwork) Register(client Client) Address {
	for i, cur := range n.clients {
		if cur == client {
			return Address(i)
		}
	}
	n.clients = append(n.clients, client)
	return Address(len(n.clients) - 1)
}

func (n *DemoNetwork) Unregister(client Client) {
	for i, cur := range n.clients {
		if cur == client {
			n.clients[i] = nil
		}
	}
}

func (n *DemoNetwork) GetAllAddresses() []Address {
	res := make([]Address, 0, len(n.clients))
	for i, cur := range n.clients {
		if cur != nil {
			res = append(res, Address(i))
		}
	}
	return res
}

func (n *DemoNetwork) Call(trg Address, msg Message) Message {
	if trg < 0 || int(trg) >= len(n.clients) || n.clients[trg] == nil {
		return nil
	}
	return n.clients[trg].Call(msg)
}

func (n *DemoNetwork) Broadcast(msg Message) {
	for _, cur := range n.clients {
		if cur != nil {
			cur.Observe(msg)
		}
	}
}
