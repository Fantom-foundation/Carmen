package common

// Releaser is an interface for types owning resources that should be released
// after use to facilitate resource re-utilization.
type Releaser interface {
	// Release releases bound resources for re-use. The object this function is
	// called on becomes invalid for any future operation afterwards.
	Release()
}
