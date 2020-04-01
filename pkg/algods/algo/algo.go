// Package algo implements helper algorithms.
package algo

// Min returns the minimum value of a and b.
func Min(a uint64, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

// Max returns the maximum value of a and b.
func Max(a uint64, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}
