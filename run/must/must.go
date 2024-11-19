package must

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"
)

func Must1(err error) {
	if err != nil {
		assert.Unreachable("[WKL] Update benchmark failed", map[string]any{"err": err.Error()})
		panic(err)
	}
}

func Must[T any](t T, err error) T {
	if err != nil {
		assert.Unreachable("[WKL] Update benchmark failed", map[string]any{"err": err.Error()})
	}
	return t
}
