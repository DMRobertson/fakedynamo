package fakedynamo_test

func ptr[T any](v T) *T {
	return &v
}

func val[T any](p *T) T {
	return *p
}
