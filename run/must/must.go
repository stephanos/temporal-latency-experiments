package must

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"
)

func Must1(err error) {
	assert.Always(err == nil, "[WKL] Update benchmark failed", map[string]any{"err": err})
	if err != nil {
		panic(err)
	}
}

func Must[T any](t T, err error) T {
	assert.Always(err == nil, "[WKL] Update benchmark failed", map[string]any{"err": err})
	if err != nil {
		panic(err)
	}
	return t
}
