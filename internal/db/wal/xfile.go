package wal

import (
	"fmt"
	"os"
	"sync/atomic"
	"syscall"
	"unsafe"
)

type xWAL struct {
}

func newXWAL() *xWAL {
	return &xWAL{}
}

func main() {
	// Open the file
	file, err := os.OpenFile("test.txt", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	// Truncate the file to 1MB
	err = file.Truncate(1024 * 1024)
	if err != nil {
		panic(err)
	}
	// Memory map the file
	mmap, err := syscall.Mmap(int(file.Fd()), 0, 1024*1024, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	defer syscall.Munmap(mmap)

	// Write a slice of bytes to the memory-mapped file using compare and swap
	oldPtr := (*byte)(unsafe.Pointer(&mmap[0]))
	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	for {
		newData := make([]byte, len(data))
		copy(newData, data)
		oldLen := int(atomic.LoadUint32((*uint32)(unsafe.Pointer(oldPtr))))
		if oldLen > 0 {
			oldData := make([]byte, oldLen)
			copy(oldData, mmap[:oldLen])
			mmap = mmap[:oldLen]
			copy(mmap, oldData)
		}
		if len(mmap) < len(newData)+4 {
			panic("Not enough space in the memory-mapped file")
		}
		atomic.StoreUint32((*uint32)(unsafe.Pointer(oldPtr)), uint32(len(newData)))
		mmap = mmap[4:]
		copy(mmap, newData)
		break
	}

	// Read the slice of bytes from the memory-mapped file
	oldLen := int(atomic.LoadUint32((*uint32)(unsafe.Pointer(oldPtr))))
	if oldLen > 0 {
		oldData := make([]byte, oldLen)
		copy(oldData, mmap[:oldLen])
		fmt.Printf("Data in memory mapped file: %v\n", oldData)
	}
}
