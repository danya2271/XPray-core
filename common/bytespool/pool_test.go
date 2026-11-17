package bytespool_test

import (
	"fmt"
	"testing"

	"github.com/xtls/xray-core/common/bytespool"
)

func TestAllocationSizeClasses(t *testing.T) {
	for _, size := range []int32{1, 2048, 2049, 4096, 4097, 8192, 8193, 16384, 16385, 32768, 32769, 65536, 65537, 131072, 131073} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			expected := int32(2048)
			for expected < size && expected < 131072 {
				expected *= 2
			}
			if size > expected {
				expected = size
			}
			for range 2 {
				buffer := bytespool.Alloc(size)
				if int32(cap(buffer)) != expected || int32(len(buffer)) < size {
					t.Fatalf("request=%d length=%d capacity=%d expected_capacity=%d", size, len(buffer), cap(buffer), expected)
				}
				bytespool.Free(buffer)
			}
		})
	}
}

func TestFreeDoesNotRetainOversizedArrays(t *testing.T) {
	for _, size := range []int32{2048, 4096, 8192, 16384, 32768, 65536, 131072} {
		bytespool.Free(make([]byte, size+1))
		for range 2 {
			buffer := bytespool.Alloc(size)
			if int32(cap(buffer)) != size {
				t.Fatalf("%d-byte pool retained oversized allocation: %d", size, cap(buffer))
			}
			bytespool.Free(buffer)
		}
	}
}
