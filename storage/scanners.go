// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package storage

import (
	"context"

	"github.com/oteldb/promql-engine/execution/model"
	"github.com/oteldb/promql-engine/logicalplan"
	"github.com/oteldb/promql-engine/query"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// SeriesCounter is the optional count-pushdown capability a [Scanners] implementation exposes
// from its underlying querier: it returns the number of series matching matchers with at least one
// sample in [startMs, endMs] (Prometheus milliseconds), without materializing samples or labels.
// It backs the PromQL `count(<selector>)` fast path. A Scanners whose querier does not support it
// returns nil from SeriesCounter and the plan falls back to the aggregate-over-Select path.
type SeriesCounter interface {
	CountSeries(ctx context.Context, startMs, endMs int64, matchers ...*labels.Matcher) (uint64, error)
}

// GroupedSeriesCounter is the grouped variant of [SeriesCounter]: it returns, per distinct value
// of label among the series matching matchers, the number of such series with at least one sample
// in [startMs, endMs] — without materializing samples or label sets. Series without the label are
// grouped under the "" key. It backs the PromQL `count by (label)(<selector>)` fast path (and,
// through the generic outer aggregation, `count(count by (label)(...))` = distinct label values).
// A Scanners whose querier does not support it returns nil from GroupedSeriesCounter and the plan
// falls back to the aggregate-over-Select path.
type GroupedSeriesCounter interface {
	CountSeriesBy(ctx context.Context, startMs, endMs int64, label string, matchers ...*labels.Matcher) (map[string]uint64, error)
}

type Scanners interface {
	Close() error
	NewVectorSelector(ctx context.Context, opts *query.Options, hints storage.SelectHints, selector logicalplan.VectorSelector) (model.VectorOperator, error)
	NewMatrixSelector(ctx context.Context, opts *query.Options, hints storage.SelectHints, selector logicalplan.MatrixSelector, call logicalplan.FunctionCall) (model.VectorOperator, error)

	// SeriesCounter returns the querier's count-pushdown capability, or nil if unsupported.
	SeriesCounter() SeriesCounter

	// GroupedSeriesCounter returns the querier's grouped-count-pushdown capability, or nil if
	// unsupported.
	GroupedSeriesCounter() GroupedSeriesCounter
}
