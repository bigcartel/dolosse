package main

import "unsafe"

func StringToByteSlice(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}