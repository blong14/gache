//go:build jemalloc
// +build jemalloc

package arena

/*
#cgo LDFLAGS: /usr/local/lib/libjemalloc.a -L/usr/local/lib -Wl,-rpath,/usr/local/lib -ljemalloc -lm -lstdc++ -pthread -ldl
#include <stdlib.h>
#include <jemalloc/jemalloc.h>
*/
import "C"
import (
	"unsafe"
)

const MaxArrayLen = 1<<50 - 1

func Calloc(n int) []byte {
	if n == 0 {
		return make([]byte, 0)
	}
	ptr := C.je_calloc(C.size_t(n), 1)
	if ptr == nil {
		panic("out of memory")
	}
	uptr := unsafe.Pointer(ptr)
	// fmt.Println("calloc")
	// Interpret the C pointer as a pointer to a Go array, then slice.
	return (*[MaxArrayLen]byte)(uptr)[:n:n]
}

func Free(b []byte) {
	if sz := cap(b); sz != 0 {
		b = b[:cap(b)]
		ptr := unsafe.Pointer(&b[0])
		C.je_free(ptr)
	}
}
