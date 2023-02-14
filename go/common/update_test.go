package common

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"testing"
)

func TestUpdateEmptyUpdateCheckReportsNoErrors(t *testing.T) {
	update := Update{}
	if err := update.Check(); err != nil {
		t.Errorf("Empty update should not report an error, but got: %v", err)
	}
}

func TestUpdateCreatedAccountsAreSortedAndMadeUniqueByNormalizer(t *testing.T) {
	addr1 := Address{0x01}
	addr2 := Address{0x02}
	addr3 := Address{0x03}

	update := Update{}
	update.AppendCreateAccount(addr2)
	update.AppendCreateAccount(addr1)
	update.AppendCreateAccount(addr3)
	update.AppendCreateAccount(addr1)

	if err := update.Normalize(); err != nil {
		t.Errorf("failed to normalize update: %v", err)
	}

	want := Update{}
	want.AppendCreateAccount(addr1)
	want.AppendCreateAccount(addr2)
	want.AppendCreateAccount(addr3)

	if !reflect.DeepEqual(want, update) {
		t.Errorf("failed to normalize create-account list, wanted %v, got %v", want.CreatedAccounts, update.CreatedAccounts)
	}
}

func TestUpdateDeletedAccountsAreSortedAndMadeUniqueByNormalizer(t *testing.T) {
	addr1 := Address{0x01}
	addr2 := Address{0x02}
	addr3 := Address{0x03}

	update := Update{}
	update.AppendDeleteAccount(addr2)
	update.AppendDeleteAccount(addr1)
	update.AppendDeleteAccount(addr3)
	update.AppendDeleteAccount(addr1)

	if err := update.Normalize(); err != nil {
		t.Errorf("failed to normalize update: %v", err)
	}

	want := Update{}
	want.AppendDeleteAccount(addr1)
	want.AppendDeleteAccount(addr2)
	want.AppendDeleteAccount(addr3)

	if !reflect.DeepEqual(want, update) {
		t.Errorf("failed to normalize deleted-account list, wanted %v, got %v", want.DeletedAccounts, update.DeletedAccounts)
	}
}

func TestUpdateBalanceUpdatesAreSortedAndMadeUniqueByNormalizer(t *testing.T) {
	addr1 := Address{0x01}
	addr2 := Address{0x02}
	addr3 := Address{0x03}

	value1 := Balance{0x01}
	value2 := Balance{0x02}
	value3 := Balance{0x03}

	update := Update{}
	update.AppendBalanceUpdate(addr2, value2)
	update.AppendBalanceUpdate(addr1, value1)
	update.AppendBalanceUpdate(addr3, value3)
	update.AppendBalanceUpdate(addr1, value1)

	if err := update.Normalize(); err != nil {
		t.Errorf("failed to normalize update: %v", err)
	}

	want := Update{}
	want.AppendBalanceUpdate(addr1, value1)
	want.AppendBalanceUpdate(addr2, value2)
	want.AppendBalanceUpdate(addr3, value3)

	if !reflect.DeepEqual(want, update) {
		t.Errorf("failed to normalize balance update list, wanted %v, got %v", want.Balances, update.Balances)
	}
}

func TestUpdateConflictingBalanceUpdatesCanNotBeNormalized(t *testing.T) {
	addr1 := Address{0x01}

	value1 := Balance{0x01}
	value2 := Balance{0x02}

	update := Update{}
	update.AppendBalanceUpdate(addr1, value1)
	update.AppendBalanceUpdate(addr1, value2)

	if err := update.Normalize(); err == nil {
		t.Errorf("normalizing conflicting updates should fail")
	}
}

func TestUpdateNonceUpdatesAreSortedAndMadeUniqueByNormalizer(t *testing.T) {
	addr1 := Address{0x01}
	addr2 := Address{0x02}
	addr3 := Address{0x03}

	value1 := Nonce{0x01}
	value2 := Nonce{0x02}
	value3 := Nonce{0x03}

	update := Update{}
	update.AppendNonceUpdate(addr2, value2)
	update.AppendNonceUpdate(addr1, value1)
	update.AppendNonceUpdate(addr3, value3)
	update.AppendNonceUpdate(addr1, value1)

	if err := update.Normalize(); err != nil {
		t.Errorf("failed to normalize update: %v", err)
	}

	want := Update{}
	want.AppendNonceUpdate(addr1, value1)
	want.AppendNonceUpdate(addr2, value2)
	want.AppendNonceUpdate(addr3, value3)

	if !reflect.DeepEqual(want, update) {
		t.Errorf("failed to normalize nonce update list, wanted %v, got %v", want.Balances, update.Balances)
	}
}

func TestUpdateConflictingNonceUpdatesCanNotBeNormalized(t *testing.T) {
	addr1 := Address{0x01}

	value1 := Nonce{0x01}
	value2 := Nonce{0x02}

	update := Update{}
	update.AppendNonceUpdate(addr1, value1)
	update.AppendNonceUpdate(addr1, value2)

	if err := update.Normalize(); err == nil {
		t.Errorf("normalizing conflicting updates should fail")
	}
}

func TestUpdateCodeUpdatesAreSortedAndMadeUniqueByNormalizer(t *testing.T) {
	addr1 := Address{0x01}
	addr2 := Address{0x02}
	addr3 := Address{0x03}

	value1 := []byte{0x01}
	value2 := []byte{0x02}
	value3 := []byte{0x03}

	update := Update{}
	update.AppendCodeUpdate(addr2, value2)
	update.AppendCodeUpdate(addr1, value1)
	update.AppendCodeUpdate(addr3, value3)
	update.AppendCodeUpdate(addr1, value1)

	if err := update.Normalize(); err != nil {
		t.Errorf("failed to normalize update: %v", err)
	}

	want := Update{}
	want.AppendCodeUpdate(addr1, value1)
	want.AppendCodeUpdate(addr2, value2)
	want.AppendCodeUpdate(addr3, value3)

	if !reflect.DeepEqual(want, update) {
		t.Errorf("failed to normalize code update list, wanted %v, got %v", want.Balances, update.Balances)
	}
}

func TestUpdateConflictingCodeUpdatesCanNotBeNormalized(t *testing.T) {
	addr1 := Address{0x01}

	value1 := []byte{0x01}
	value2 := []byte{0x02}

	update := Update{}
	update.AppendCodeUpdate(addr1, value1)
	update.AppendCodeUpdate(addr1, value2)

	if err := update.Normalize(); err == nil {
		t.Errorf("normalizing conflicting updates should fail")
	}
}

func TestUpdateSlotUpdatesAreSortedAndMadeUniqueByNormalizer(t *testing.T) {
	addr1 := Address{0x01}
	addr2 := Address{0x02}
	addr3 := Address{0x03}

	key1 := Key{0x01}
	key2 := Key{0x02}
	key3 := Key{0x03}

	value1 := Value{0x01}
	value2 := Value{0x02}
	value3 := Value{0x03}

	update := Update{}
	update.AppendSlotUpdate(addr2, key2, value2)
	update.AppendSlotUpdate(addr1, key1, value1)
	update.AppendSlotUpdate(addr3, key3, value3)
	update.AppendSlotUpdate(addr1, key1, value1)

	if err := update.Normalize(); err != nil {
		t.Errorf("failed to normalize update: %v", err)
	}

	want := Update{}
	want.AppendSlotUpdate(addr1, key1, value1)
	want.AppendSlotUpdate(addr2, key2, value2)
	want.AppendSlotUpdate(addr3, key3, value3)

	if !reflect.DeepEqual(want, update) {
		t.Errorf("failed to normalize slot update list, wanted %v, got %v", want.Balances, update.Balances)
	}
}

func TestUpdateConflictingSlotUpdatesCanNotBeNormalized(t *testing.T) {
	addr1 := Address{0x01}
	key1 := Key{0x01}

	value1 := Value{0x01}
	value2 := Value{0x02}

	update := Update{}
	update.AppendSlotUpdate(addr1, key1, value1)
	update.AppendSlotUpdate(addr1, key1, value2)

	if err := update.Normalize(); err == nil {
		t.Errorf("normalizing conflicting updates should fail")
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
		func(u *Update) { u.AppendCreateAccount(Address{0x01}) },
		func(u *Update) { u.AppendCreateAccount(Address{0x02}) },
		func(u *Update) { u.AppendCreateAccount(Address{0x03}) },
	},
	{
		"DeleteAccount",
		func(u *Update) { u.AppendDeleteAccount(Address{0x01}) },
		func(u *Update) { u.AppendDeleteAccount(Address{0x02}) },
		func(u *Update) { u.AppendDeleteAccount(Address{0x03}) },
	},
	{
		"UpdateBalance",
		func(u *Update) { u.AppendBalanceUpdate(Address{0x01}, Balance{}) },
		func(u *Update) { u.AppendBalanceUpdate(Address{0x02}, Balance{}) },
		func(u *Update) { u.AppendBalanceUpdate(Address{0x03}, Balance{}) },
	},
	{
		"UpdateNonce",
		func(u *Update) { u.AppendNonceUpdate(Address{0x01}, Nonce{}) },
		func(u *Update) { u.AppendNonceUpdate(Address{0x02}, Nonce{}) },
		func(u *Update) { u.AppendNonceUpdate(Address{0x03}, Nonce{}) },
	},
	{
		"UpdateCode",
		func(u *Update) { u.AppendCodeUpdate(Address{0x01}, []byte{}) },
		func(u *Update) { u.AppendCodeUpdate(Address{0x02}, []byte{}) },
		func(u *Update) { u.AppendCodeUpdate(Address{0x03}, []byte{}) },
	},
	{
		"UpdateSlot",
		func(u *Update) { u.AppendSlotUpdate(Address{0x01}, Key{0x00}, Value{}) },
		func(u *Update) { u.AppendSlotUpdate(Address{0x02}, Key{0x00}, Value{}) },
		func(u *Update) { u.AppendSlotUpdate(Address{0x02}, Key{0x01}, Value{}) },
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

func TestUpdateCreatingAndDeletingSameAccountIsInvalid(t *testing.T) {
	addr := Address{0x01}

	update := Update{}
	update.AppendCreateAccount(addr)
	if update.Check() != nil {
		t.Errorf("just creating an account should be fine")
	}
	update.AppendDeleteAccount(addr)
	if update.Check() == nil {
		t.Errorf("creating and deleting the same account should fail")
	}
}

func TestUpdateSingleAccountCreatedAndDeletedIsDetectedAlsoWhenPartOfAList(t *testing.T) {
	update := Update{}
	for i := 0; i < 10; i++ {
		addr := Address{byte(i)}
		if i%2 == 0 {
			update.AppendCreateAccount(addr)
		} else {
			update.AppendDeleteAccount(addr)
		}
	}

	if err := update.Check(); err != nil {
		t.Errorf("non-overlapping create and delete list should be fine, but got: %v", err)
	}

	update.AppendCreateAccount(Address{9})

	if update.Check() == nil {
		t.Errorf("creating and deleting the same account should fail")
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

	update.AppendDeleteAccount(Address{0xA1})
	update.AppendDeleteAccount(Address{0xA2})

	update.AppendCreateAccount(Address{0xB1})
	update.AppendCreateAccount(Address{0xB2})
	update.AppendCreateAccount(Address{0xB3})

	update.AppendBalanceUpdate(Address{0xC1}, Balance{0x01})
	update.AppendBalanceUpdate(Address{0xC2}, Balance{0x02})

	update.AppendNonceUpdate(Address{0xD1}, Nonce{0x03})
	update.AppendNonceUpdate(Address{0xD2}, Nonce{0x04})

	update.AppendCodeUpdate(Address{0xE1}, []byte{})
	update.AppendCodeUpdate(Address{0xE2}, []byte{0x01})
	update.AppendCodeUpdate(Address{0xE3}, []byte{0x02, 0x03})

	update.AppendSlotUpdate(Address{0xF1}, Key{0x01}, Value{0xA1})
	update.AppendSlotUpdate(Address{0xF2}, Key{0x02}, Value{0xA2})
	update.AppendSlotUpdate(Address{0xF3}, Key{0x03}, Value{0xB1})
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
			"61126de1b795b976f3ac878f48e88fa77a87d7308ba57c7642b9e1068403a496",
		},
		{
			getExampleUpdate(),
			"5524cf120229bc99ef762303b408900b8998f1d725958e62ac49fea69a5f7901",
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
