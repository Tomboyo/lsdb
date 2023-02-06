package support

func Reverse[T interface{}](slice []T) []T {
	result := make([]T, len(slice))

	for in, out := 0, len(slice)-1; in < len(slice); in, out = in+1, out-1 {
		result[out] = slice[in]
	}
	return result
}
