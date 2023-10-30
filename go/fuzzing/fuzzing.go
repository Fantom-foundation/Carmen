package fuzzing

import (
	"testing"
)

//go:generate mockgen -source fuzzing.go -destination fuzzing_mocks.go -package fuzzing

// Operation represents one step applied to the system under a fuzzing campaign.
// Each operation must be serialisable to a byte array, which allows for
// carrying the operation code and data. This serialised form is used
// for passing the operation into the fuzzer.
type Operation[T any] interface {
	// Apply is executed by the fuzzer for each operation to
	// perform a required action to the system under test.
	// The context passed to this method carries the system under test
	// and its state. It is state-full to move from one step to another.
	Apply(context *T)

	// Serialize converts this operation to a byte array to be passed to the fuzzer.
	// The output format is not defined, but it must be readable by deserialiser
	// in Campaign.Deserialize().
	// Typically, it contains a code of this operation in the first byte, followed
	// by payload in next bytes.
	Serialize() []byte
}

// OperationSequence is a chain of operations.
type OperationSequence[T any] []Operation[T]

// Campaign maintains one fuzzing campaign. It contains methods
// to initialise and finalise data of the campaign.
// It is passed to the fuzzer as a factory to create Operations to seed the fuzzer,
// to create the context passed through each step of the fuzzing campaign,
// and finally allows for cleaning-up at the end of the campaign.
type Campaign[T any] interface {
	// Init is used for the initialization of the campaign before it starts.
	// In particular, a set of operation sequences is returned
	// to be used for seeding the fuzzer.
	// One OperationSequence is one seed for the fuzzer. Many seeds may be created,
	// i.e. this method returns an array.
	// This method is called once before the campaign starts.
	Init() []OperationSequence[T]

	// CreateContext creates a state full object that holds the system under fuzzing
	// plus state information that is needed to be hold between each steps of the campaign.
	// This context is passed to each Operation.Apply() function during the campaign.
	// This method is called once per campaign loop iteration. The campaign loop
	// is one execution exercising the system under test with Operations derived from the seed
	// created by Init().
	CreateContext(t *testing.T) *T

	// Deserialize interprets a byte array generated by the fuzzer out of the initial seed.
	// This byte array represent a set of operations that are  decoded and returned.
	// The fuzzer loops over these operations
	// amd applies changes defined in them by calling Operation.Apply().
	// This method is called once per each campaign loop to convert binary representation of Operations.
	Deserialize(*testing.T, []byte) []Operation[T]

	// Cleanup gets the context passed through this campaign and allows for closing and cleaning it.
	// This method is called once per each campaign loop. T
	Cleanup(t *testing.T, context *T)
}

// TestingF is an interface covering some of the methods of testing.F struct.
// It is provided for easy mocking.
type TestingF interface {
	Add(args ...any)
	Fuzz(ff any)
}

// Fuzz perform fuzzing campaign. It initialises the campaign using the input interface Campaign
// and forwards to the build-in testing.F behind the TestingF interface.
// The campaign is initialised by calling Campaign.Init to generate chains of operations.
// These operations are the seed of the campaign. which is then executed in many loops and is multithreaded.
// Each loop runs a set of operations derived by the fuzzer and converted to operations by Campaign.Deserialize.
// The operations manipulate the system under test and update its state using the context
// from Campaign.CreateContext. Each loop is finalised by calling Campaign.Cleanup().
func Fuzz[T any](f TestingF, c Campaign[T]) {

	// convert operations to seed.
	for _, opSet := range c.Init() {
		var raw []byte
		for _, op := range opSet {
			raw = append(raw, op.Serialize()...)
		}
		f.Add(raw)
	}

	f.Fuzz(func(t *testing.T, rawData []byte) {
		fuzz(t, c, rawData)
	})
}

func fuzz[T any](t *testing.T, c Campaign[T], rawData []byte) {
	ctx := c.CreateContext(t)
	for _, op := range c.Deserialize(t, rawData) {
		op.Apply(ctx)
	}
	c.Cleanup(t, ctx)
}
