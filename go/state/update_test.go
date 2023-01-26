package state

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"testing"

	"github.com/Fantom-foundation/Carmen/go/common"
)

func TestUpdateEmptyUpdateCheckReportsNoErrors(t *testing.T) {
	update := Update{}
	if err := update.Check(); err != nil {
		t.Errorf("Empty update should not report an error, but got: %v", err)
	}
}

// updateValueCase are used to test for all fields in the Update class that Check() is
// detecting ordering or uniqueness issues.
var updateValueCase = []struct {
	target       string
	appendFirst  func(u *Update)
	appendSecond func(u *Update)
	appendThird  func(u *Update)
}{
	{
		"CreateAccount",
		func(u *Update) { u.AppendCreateAccount(common.Address{0x01}) },
		func(u *Update) { u.AppendCreateAccount(common.Address{0x02}) },
		func(u *Update) { u.AppendCreateAccount(common.Address{0x03}) },
	},
	{
		"DeleteAccount",
		func(u *Update) { u.AppendDeleteAccount(common.Address{0x01}) },
		func(u *Update) { u.AppendDeleteAccount(common.Address{0x02}) },
		func(u *Update) { u.AppendDeleteAccount(common.Address{0x03}) },
	},
	{
		"UpdateBalance",
		func(u *Update) { u.AppendBalanceUpdate(common.Address{0x01}, common.Balance{}) },
		func(u *Update) { u.AppendBalanceUpdate(common.Address{0x02}, common.Balance{}) },
		func(u *Update) { u.AppendBalanceUpdate(common.Address{0x03}, common.Balance{}) },
	},
	{
		"UpdateNonce",
		func(u *Update) { u.AppendNonceUpdate(common.Address{0x01}, common.Nonce{}) },
		func(u *Update) { u.AppendNonceUpdate(common.Address{0x02}, common.Nonce{}) },
		func(u *Update) { u.AppendNonceUpdate(common.Address{0x03}, common.Nonce{}) },
	},
	{
		"UpdateCode",
		func(u *Update) { u.AppendCodeUpdate(common.Address{0x01}, []byte{}) },
		func(u *Update) { u.AppendCodeUpdate(common.Address{0x02}, []byte{}) },
		func(u *Update) { u.AppendCodeUpdate(common.Address{0x03}, []byte{}) },
	},
	{
		"UpdateSlot",
		func(u *Update) { u.AppendSlotUpdate(common.Address{0x01}, common.Key{0x00}, common.Value{}) },
		func(u *Update) { u.AppendSlotUpdate(common.Address{0x02}, common.Key{0x00}, common.Value{}) },
		func(u *Update) { u.AppendSlotUpdate(common.Address{0x02}, common.Key{0x01}, common.Value{}) },
	},
}

func TestUpdateDuplicatesAreDetected(t *testing.T) {
	for _, test := range updateValueCase {
		t.Run(test.target, func(t *testing.T) {
			update := Update{}
			test.appendFirst(&update)
			if err := update.Check(); err != nil {
				t.Errorf("creating a single account should not fail the check, but got: %v", err)
			}
			test.appendFirst(&update)
			if err := update.Check(); err == nil {
				t.Errorf("expected duplicate to be detected, but Check() passed")
			}
		})
	}
}

func TestUpdateOutOfOrderUpdatesAreDetected(t *testing.T) {
	for _, test := range updateValueCase {
		t.Run(test.target, func(t *testing.T) {
			update := Update{}
			test.appendSecond(&update)
			test.appendThird(&update)
			if err := update.Check(); err != nil {
				t.Errorf("ordered updates should pass, but got %v", err)
			}
			test.appendFirst(&update)
			if err := update.Check(); err == nil {
				t.Errorf("out-of-ordered updates should be detected, but Check() passed")
			}
		})
	}
}

func TestUpdateEmptyUpdateCanBeSerializedAndDeserialized(t *testing.T) {
	update := Update{}

	data := update.ToBytes()
	restored, err := UpdateFromBytes(data)
	if err != nil {
		t.Errorf("failed to parse encoded update: %v", err)
	}
	if !reflect.DeepEqual(update, restored) {
		t.Errorf("restored update is not the same as original\noriginal: %+v\nrestored: %+v", update, restored)
	}
}

func getExampleUpdate() Update {
	update := Update{}

	update.AppendDeleteAccount(common.Address{0xA1})
	update.AppendDeleteAccount(common.Address{0xA2})

	update.AppendCreateAccount(common.Address{0xB1})
	update.AppendCreateAccount(common.Address{0xB2})
	update.AppendCreateAccount(common.Address{0xB3})

	update.AppendBalanceUpdate(common.Address{0xC1}, common.Balance{0x01})
	update.AppendBalanceUpdate(common.Address{0xC2}, common.Balance{0x02})

	update.AppendNonceUpdate(common.Address{0xD1}, common.Nonce{0x03})
	update.AppendNonceUpdate(common.Address{0xD2}, common.Nonce{0x04})

	update.AppendCodeUpdate(common.Address{0xE1}, []byte{})
	update.AppendCodeUpdate(common.Address{0xE2}, []byte{0x01})
	update.AppendCodeUpdate(common.Address{0xE3}, []byte{0x02, 0x03})

	update.AppendSlotUpdate(common.Address{0xF1}, common.Key{0x01}, common.Value{0xA1})
	update.AppendSlotUpdate(common.Address{0xF2}, common.Key{0x02}, common.Value{0xA2})
	update.AppendSlotUpdate(common.Address{0xF3}, common.Key{0x03}, common.Value{0xB1})
	return update
}

func TestUpdateDeserializationAndRestoration(t *testing.T) {
	update := getExampleUpdate()
	data := update.ToBytes()
	restored, err := UpdateFromBytes(data)
	if err != nil {
		t.Errorf("failed to parse encoded update: %v", err)
	}
	if !reflect.DeepEqual(update, restored) {
		t.Errorf("restored update is not the same as original\noriginal: %+v\nrestored: %+v", update, restored)
	}
}

func TestUpdateParsingEmptyBytesFailsWithError(t *testing.T) {
	_, err := UpdateFromBytes([]byte{})
	if err == nil {
		t.Errorf("parsing empty byte sequence should fail")
	}
}

func TestUpdateParsingInvalidVersionNumberShouldFail(t *testing.T) {
	data := make([]byte, 200)
	data[0] = updateEncodingVersion + 1
	_, err := UpdateFromBytes(data)
	if err == nil {
		t.Errorf("parsing should detect invalid version number")
	}
}

func TestUpdateParsingTruncatedDataShouldFailWithError(t *testing.T) {
	update := getExampleUpdate()
	data := update.ToBytes()
	// Test that no panic is triggered.
	for i := 0; i < len(data); i++ {
		if _, err := UpdateFromBytes(data[0:i]); err == nil {
			t.Errorf("parsing of truncated data should fail")
		}
	}
	if _, err := UpdateFromBytes(data); err != nil {
		t.Errorf("unable to parse full encoding")
	}
}

func TestUpdateKnownEncodings(t *testing.T) {
	testCases := []struct {
		update Update
		hash   string
	}{
		{
			Update{},
			"dd46c3eebb1884ff3b5258c0a2fc9398e560a29e0780d4b53869b6254aa46a96",
		},
		{
			getExampleUpdate(),
			"bc283c81ee1607c83e557420bf3763ab99aca2a59a99d0c66d7105e1ff2fea26",
		},
	}
	for _, test := range testCases {
		hasher := sha256.New()
		hasher.Write(test.update.ToBytes())
		hash := fmt.Sprintf("%x", hasher.Sum(nil))
		if hash != test.hash {
			t.Errorf("invalid encoding, expected hash %v, got %v", test.hash, hash)
		}
	}
}
