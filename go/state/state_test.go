package state

import (
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

type namedStateConfig struct {
	name        string
	createState func(tmpDir string) (State, error)
}

func initStates() []namedStateConfig {
	var res []namedStateConfig
	for _, s := range initCppStates() {
		res = append(res, namedStateConfig{name: "cpp-" + s.name, createState: s.createState})
	}
	for _, s := range initGoStates() {
		res = append(res, namedStateConfig{name: "go-" + s.name, createState: s.createState})
	}
	return res
}

func testEachConfiguration(t *testing.T, test func(s State)) {
	for _, config := range initStates() {
		t.Run(config.name, func(t *testing.T) {
			state, err := config.createState(t.TempDir())
			if err != nil {
				t.Fatalf("failed to initialize state %s", config.name)
			}
			defer state.Close()

			test(state)
		})
	}
}

func testHashAfterModification(t *testing.T, mod func(s State)) {
	ref, err := NewMemory()
	if err != nil {
		t.Fatalf("failed to create reference state: %v", err)
	}
	mod(ref)
	want, err := ref.GetHash()
	if err != nil {
		t.Fatalf("failed to get hash of reference state: %v", err)
	}
	testEachConfiguration(t, func(state State) {
		mod(state)
		got, err := state.GetHash()
		if err != nil {
			t.Fatalf("failed to compute hash: %v", err)
		}
		if want != got {
			t.Errorf("Invalid hash, wanted %v, got %v", want, got)
		}
	})
}

func TestEmptyHash(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		// nothing
	})
}

func TestAddressHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.CreateAccount(address1)
	})
}

func TestMultipleAddressHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.CreateAccount(address1)
		s.CreateAccount(address2)
		s.CreateAccount(address3)
	})
}

func TestDeletedAddressHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.CreateAccount(address1)
		s.CreateAccount(address2)
		s.CreateAccount(address3)
		s.DeleteAccount(address1)
		s.DeleteAccount(address2)
	})
}

func TestStorageHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.SetStorage(address1, key2, val3)
	})
}

func TestMultipleStorageHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.SetStorage(address1, key2, val3)
		s.SetStorage(address2, key3, val1)
		s.SetStorage(address3, key1, val2)
	})
}

func TestBalanceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.SetBalance(address1, balance1)
	})
}

func TestMultipleBalanceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.SetBalance(address1, balance1)
		s.SetBalance(address2, balance2)
		s.SetBalance(address3, balance3)
	})
}

func TestNonceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.SetNonce(address1, nonce1)
	})
}

func TestMultipleNonceUpdateHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		s.SetNonce(address1, nonce1)
		s.SetNonce(address2, nonce2)
		s.SetNonce(address3, nonce3)
	})
}

func TestLargeStateHashes(t *testing.T) {
	testHashAfterModification(t, func(s State) {
		for i := 0; i < 100; i++ {
			address := common.Address{byte(i)}
			s.CreateAccount(address)
			for j := 0; j < 100; j++ {
				key := common.Key{byte(j)}
				s.SetStorage(address, key, common.Value{byte(i), 0, 0, byte(j)})
			}
			if i%21 == 0 {
				s.DeleteAccount(address)
			}
			s.SetBalance(address, common.Balance{byte(i)})
			s.SetNonce(address, common.Nonce{byte(i + 1)})
		}
	})
}

func TestCanComputeNonEmptyMemoryFootprint(t *testing.T) {
	testEachConfiguration(t, func(s State) {
		fp := s.GetMemoryFootprint()
		if fp == nil {
			t.Fatalf("state produces invalid footprint: %v", fp)
		}
		if fp.Total() <= 0 {
			t.Errorf("memory footprint should not be empty")
		}
		if _, err := fp.ToString("top"); err != nil {
			t.Errorf("Unable to print footprint: %v", err)
		}
	})
}
