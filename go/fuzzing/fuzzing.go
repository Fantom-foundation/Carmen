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
	Apply(t *testing.T, context *T)

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
	// This byte array represents a list of operations that are decoded and returned.
	// The fuzzer loops over these operations
	// and applies changes defined in them by calling Operation.Apply().
	// This method is called once for each campaign loop to convert binary representation of Operations.
	Deserialize([]byte) []Operation[T]

	// Cleanup gets the context passed through this campaign and allows for closing and cleaning it.
	// This method is called once for each campaign loop.
	Cleanup(t *testing.T, context *T)
}

// TestingF is an interface covering some of the methods of testing.F struct.
// It is provided for easy mocking.
type TestingF interface {
	Add(args ...any)
	Fuzz(ff any)
}

// Fuzz performs a fuzzing campaign.
// The campaign is initialised by calling Campaign.Init to generate chains of operations.
// These operations are the seed of the campaign. which is then executed repeatedly and concurrently.
// Each execution runs a set of operations derived by the fuzzer and converted to operations by Campaign.Deserialize.
// The operations manipulate the system under test and update its state using the context
// produced by Campaign.CreateContext. Each execution is finalised by calling Campaign.Cleanup().
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
	for _, op := range c.Deserialize(rawData) {
		op.Apply(t, ctx)
	}
	c.Cleanup(t, ctx)
}

type Serializable interface {
	Serialize() []byte
}

// FuzzOp is a default fuzzing Operation, which defines a callback
// method to implement an action for the operation.
// Furthermore, it defines initial data of this operation.
// This seed data is returned by the Serialise method, while the Apply method
// routes to the defined callback method.
type FuzzOp[T ~byte, C any, PAYLOAD Serializable] struct {
	opType T
	data   PAYLOAD
	apply  func(opType T, data PAYLOAD, t *testing.T, context *C)
}

// NewOp creates a new fuzzing operation with predefined action amd initial data.
// It gets identifier of this operation, initial data, fuzzing context, and the callback method
// executed every time this operation is executed.
func NewOp[T ~byte, C any, PAYLOAD Serializable](
	opType T,
	data PAYLOAD,
	apply func(opType T, data PAYLOAD, t *testing.T, context *C)) *FuzzOp[T, C, PAYLOAD] {

	return &FuzzOp[T, C, PAYLOAD]{
		opType: opType,
		data:   data,
		apply:  apply,
	}
}

// Serialize converts this operation to a byte array.
// It contains identifier of this operation in the first byte, followed
// by payload in consecutive bytes.
func (op *FuzzOp[T, C, PAYLOAD]) Serialize() []byte {
	return append([]byte{byte(op.opType)}, op.data.Serialize()...)
}

// Apply passes the call to the apply callback method.
func (op *FuzzOp[T, C, PAYLOAD]) Apply(t *testing.T, c *C) {
	op.apply(op.opType, op.data, t, c)
}

// EmptyPayload is a convenient implementation of empty payload
type EmptyPayload struct{}

func (p EmptyPayload) Serialize() []byte {
	return []byte{}
}

// SerialisedPayload that is a type of payload that is directly represented as a byte array,
// together with the original value.
type SerialisedPayload[T any] struct {
	Val        T
	serialised []byte
}

// NewSerialisedPayload creates a SerialisedPayload that is a type of payload that is directly represented as a byte array,
// together with the original value.
func NewSerialisedPayload[T any](payload T, serialised []byte) SerialisedPayload[T] {
	return SerialisedPayload[T]{
		Val:        payload,
		serialised: serialised,
	}
}

func (p SerialisedPayload[T]) Serialize() []byte {
	return p.serialised
}

// OpsFactoryRegistry is a convenient implementation allowing the client to register factories
// to create fuzzing operations. The instances of operations can be obtained from this registry then.
type OpsFactoryRegistry[T ~byte, C any] map[T]func(payload any) Operation[C]

func NewRegistry[T ~byte, C any]() OpsFactoryRegistry[T, C] {
	return make(map[T]func(payload any) Operation[C])
}

func (r OpsFactoryRegistry[T, C]) register(opType T, fact func(payload any) Operation[C]) {
	r[opType] = fact
}

func (r OpsFactoryRegistry[T, C]) CreateDataOp(opType T, payload any) Operation[C] {
	return r[opType](payload)
}

func (r OpsFactoryRegistry[T, C]) CreateEmptyDataOp(opType T) Operation[C] {
	return r[opType](nil)
}

// RegisterOp registers a factory to create a fuzzing operation based for an operation op-code.
func RegisterOp[T ~byte, C any, D any](registry OpsFactoryRegistry[T, C], opType T, fact func(payload D) Operation[C]) {
	registry.register(opType, func(payload any) Operation[C] {
		return fact(payload.(D))
	})
}

// RegisterEmptyDataOp registers a factory to create a fuzzing operation based for an operation op-code.
// This method registers an operation that has only its opcode, but no payload.
func RegisterEmptyDataOp[T ~byte, C any](registry OpsFactoryRegistry[T, C], opType T, apply func(opType T, t *testing.T, context *C)) {
	registry.register(opType, func(payload any) Operation[C] {
		adapted := func(opType T, _ EmptyPayload, t *testing.T, context *C) {
			apply(opType, t, context)
		}
		return NewOp(opType, EmptyPayload{}, adapted)
	})
}

// RegisterDataOp registers a factory to create a fuzzing operation based for an operation op-code.
// This method registers an operation that has its opcode together with a payload.
func RegisterDataOp[T ~byte, C any, D any](registry OpsFactoryRegistry[T, C], opType T, serialise func(payload D) []byte, applyOp func(opType T, data D, t *testing.T, context *C)) {
	registry.register(opType, func(payload any) Operation[C] {
		adapted := func(opType T, data SerialisedPayload[D], t *testing.T, context *C) {
			applyOp(opType, data.Val, t, context)
		}
		return NewOp(opType, NewSerialisedPayload[D](payload.(D), serialise(payload.(D))), adapted)
	})
}
