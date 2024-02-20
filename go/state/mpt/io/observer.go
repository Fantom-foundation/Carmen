package io

// Observer is a type definition for outside observers of IO operations.
type Observer interface {
	Notify(string)
}
