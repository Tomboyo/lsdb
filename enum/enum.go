package enum

func Map[A interface{}, B interface{}](slice []A, f func(a A) B) []B {
	result := make([]B, len(slice))
	for index, a := range slice {
		result[index] = f(a)
	}
	return result
}
