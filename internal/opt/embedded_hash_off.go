//go:build !cc_embedded_hash

package opt

const EmbeddedHash_ = false

type EmbeddedHash struct{}

//go:nosplit
func (e *EmbeddedHash) GetHash() uintptr {
	return 0
}

//go:nosplit
func (e *EmbeddedHash) SetHash(_ uintptr) {
}
