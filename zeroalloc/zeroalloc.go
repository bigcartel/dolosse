package zeroalloc

import "unsafe"

func StringToByteSlice(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

func ByteSliceToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func ByteSliceSliceToStringSlice(b [][]byte) []string {
	s := make([]string, len(b))

	for i := range b {
		s[i] = ByteSliceToString(b[i])
	}

	return s
}
