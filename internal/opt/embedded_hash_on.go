//go:build cc_embedded_hash

package opt

const EmbeddedHash_ = true

type EmbeddedHash struct {
	Hash uintptr
}

//go:nosplit
func (e *EmbeddedHash) GetHash() uintptr {
	return e.Hash
}

//go:nosplit
func (e *EmbeddedHash) SetHash(h uintptr) {
	e.Hash = h
}
