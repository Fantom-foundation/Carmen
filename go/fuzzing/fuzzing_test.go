package fuzzing

import (
	"go.uber.org/mock/gomock"
	"golang.org/x/exp/slices"
	"testing"
)

func TestFuzz_TwoFuzzingLoopOneCampaignSeedOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	campaign := NewMockCampaign[testContext](ctrl)
	testingF := NewMockTestingF(ctrl)

	chain1 := []Operation[testContext]{testOp(0xA), testOp(0xB), testOp(0xC)}
	chain2 := []Operation[testContext]{testOp(0x1), testOp(0x2)}
	chain3 := []Operation[testContext]{testOp(0x3)}
	chains := []OperationSequence[testContext]{chain1, chain2, chain3}

	terminalSymbol := byte(0xFA)

	// ini complete test campaign
	campaign.EXPECT().Init().Return(chains)
	// init every loop of the campaign
	context := &testContext{make([]testOp, 0, 6)}
	campaign.EXPECT().CreateContext(t).Times(2).Return(context) // three campaign loops
	campaign.EXPECT().Deserialize(t, gomock.Any()).Times(2).DoAndReturn(func(t *testing.T, raw []byte) []Operation[testContext] {
		ops := make([]Operation[testContext], 0, len(raw))
		for i := 0; i < len(raw); i++ {
			ops = append(ops, testOp(raw[i]))
		}
		return ops
	})
	campaign.EXPECT().Cleanup(t, gomock.Any()).Times(2).Do(func(t *testing.T, ctx *testContext) {
		ctx.executed = append(ctx.executed, testOp(terminalSymbol))
		terminalSymbol++
	})

	// initialisation of three chains expected, one fuzz campaign executed in total for all seed values.
	chainRawData := make([]byte, 0, 6)
	testingF.EXPECT().Add(gomock.Any()).Times(3).Do(func(rawData []byte) {
		chainRawData = append(chainRawData, rawData...)
	})
	// run fuzzing in two loops with the same seeds (no extra generated values)
	testingF.EXPECT().Fuzz(gomock.Any()).Times(1).Do(func(ff func(*testing.T, []byte)) {
		ff(t, chainRawData)
		ff(t, chainRawData)
	})

	// Run fuzzing
	Fuzz[testContext](testingF, campaign)

	// we test that all operations were called, and extended with closing symbol.
	want := []testOp{
		testOp(0xA), testOp(0xB), testOp(0xC), testOp(0x1), testOp(0x2), testOp(0x3), testOp(0xFA), // first loop
		testOp(0xA), testOp(0xB), testOp(0xC), testOp(0x1), testOp(0x2), testOp(0x3), testOp(0xFB), // second loop - different terminal symbol
	}
	got := context.executed

	if !slices.Equal(got, want) {
		t.Errorf("Executed chain of operations not valied: \n got: %v\n want: %v", got, want)
	}
}

type testContext struct {
	executed []testOp
}

type testOp byte

func (op testOp) Apply(c *testContext) {
	// only accumulate what was executed
	c.executed = append(c.executed, op)
}

func (op testOp) Serialize() []byte {
	return []byte{byte(op)}
}
