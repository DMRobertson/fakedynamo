package fakedynamo_test

import "cmp"

func ptr[T any](v T) *T {
	return &v
}

func val[T any](p *T) T {
	return *p
}

func comparePtr[T cmp.Ordered](a, b *T) int {
	return cmp.Compare(*a, *b)
}
