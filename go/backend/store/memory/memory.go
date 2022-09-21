package memory

import "github.com/Fantom-foundation/Carmen/go/common"

type Memory[I common.Identifier, V common.Serializable] struct {
}

func (m Memory[I, V]) Set(id I, value V) {
	//TODO implement me
	panic("implement me")
}

func (m Memory[I, V]) Get(id I) V {
	//TODO implement me
	panic("implement me")
}

func (m Memory[I, V]) Contains(id I) bool {
	//TODO implement me
	panic("implement me")
}

func (m Memory[I, V]) GetStateHash() common.Hash {
	//TODO implement me
	panic("implement me")
}
