package cache

func Uint64Compare(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a == b:
		return 0
	case a > b:
		return 1
	default:
		panic("error")
	}
}

func Uint64Equals(a, b uint64) bool {
	return a == b
}
