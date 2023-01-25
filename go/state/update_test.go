package state

import (
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
