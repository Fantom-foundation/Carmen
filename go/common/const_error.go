package common

// ConstError is a error type that can be used to define immutable
// error constants.
type ConstError string

func (e ConstError) Error() string {
	return string(e)
}
