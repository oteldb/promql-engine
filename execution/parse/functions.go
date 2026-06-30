// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package parse

import (
	"fmt"
	"maps"
	"sync"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/promql/parser"
)

var XFunctions = map[string]*parser.Function{
	"xdelta": {
		Name:       "xdelta",
		ArgTypes:   []parser.ValueType{parser.ValueTypeMatrix},
		ReturnType: parser.ValueTypeVector,
	},
	"xincrease": {
		Name:       "xincrease",
		ArgTypes:   []parser.ValueType{parser.ValueTypeMatrix},
		ReturnType: parser.ValueTypeVector,
	},
	"xrate": {
		Name:       "xrate",
		ArgTypes:   []parser.ValueType{parser.ValueTypeMatrix},
		ReturnType: parser.ValueTypeVector,
	},
}

var registerXFunctionsOnce sync.Once

// RegisterXFunctions makes the custom x-functions (xrate, xincrease, xdelta)
// available to the PromQL parser by adding them to the global parser.Functions
// table. Since prometheus v0.312 the parser no longer accepts a custom function
// map at construction time, so this global registration is the only way to make
// the engine parse these functions.
func RegisterXFunctions() {
	registerXFunctionsOnce.Do(func() {
		maps.Copy(parser.Functions, XFunctions)
	})
}

// IsExtFunction is a convenience function to determine whether extended range calculations are required.
func IsExtFunction(functionName string) bool {
	_, ok := XFunctions[functionName]
	return ok
}

func UnknownFunctionError(name string) error {
	msg := fmt.Sprintf("unknown function: %s", name)
	if _, ok := parser.Functions[name]; ok {
		return errors.Wrap(ErrNotImplemented, msg)
	}

	return errors.Wrap(ErrNotSupportedExpr, msg)
}
