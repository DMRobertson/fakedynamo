package fakedynamo_test

import (
	"cmp"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func ptr[T any](v T) *T {
	return &v
}

func val[T any](p *T) T {
	return *p
}

func comparePtr[T cmp.Ordered](a, b *T) int {
	return cmp.Compare(*a, *b)
}

func assertErrorContains(t *testing.T, err error, needles ...string) {
	t.Helper()
	if !assert.Error(t, err) {
		return
	}
	for _, needle := range needles {
		err = errors.New(strings.ToLower(err.Error()))
		assert.ErrorContains(t, err, strings.ToLower(needle))
	}
}
