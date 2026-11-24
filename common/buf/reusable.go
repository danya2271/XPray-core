package buf

// ReusableBuffer stores one owned buffer's metadata. Reset is valid only after
// the previous buffer has been released. The caller synchronizes reuse with
// the release callback; a released Buffer pointer must no longer be used.
type ReusableBuffer struct {
	buffer Buffer
	owner  ownership
}

func (r *ReusableBuffer) Reset(data []byte, release func()) *Buffer {
	if release == nil {
		panic("nil buffer release callback")
	}
	if r.buffer.v != nil || r.buffer.ownership != nil {
		panic("reusing a live buffer")
	}
	r.owner.release = release
	r.buffer = Buffer{v: data, end: int32(len(data)), ownership: &r.owner}
	return &r.buffer
}
