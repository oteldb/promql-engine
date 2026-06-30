// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package aggregate

import (
	"context"
	"testing"
	"time"

	"github.com/oteldb/promql-engine/execution/model"
	"github.com/oteldb/promql-engine/query"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeCounter records every (startMs, endMs) window CountSeries is asked for and returns count.
type fakeCounter struct {
	count uint64
	calls []window
}

type window struct{ startMs, endMs int64 }

func (f *fakeCounter) CountSeries(_ context.Context, startMs, endMs int64, _ ...*labels.Matcher) (uint64, error) {
	f.calls = append(f.calls, window{startMs, endMs})

	return f.count, nil
}

// TestCountSelectorInstantLookback is the regression test for the lookback-clamp bug: an instant
// count(selector) at T must ask CountSeries for [T-lookback, T], not [T, T]. Before the fix the
// clamp `if start < o.mint { start = o.mint }` collapsed the window (mint == T for an instant
// query), so only series with a sample at exactly that millisecond counted and instant count(...)
// returned the empty vector / zero on real data.
func TestCountSelectorInstantLookback(t *testing.T) {
	const lookback = 5 * time.Minute
	evalT := time.Unix(1000, 0)
	opts := &query.Options{
		Start:         evalT,
		End:           evalT,
		Step:          0, // instant
		LookbackDelta: lookback,
	}

	fc := &fakeCounter{count: 7}
	op := NewCountSelector(fc, nil, opts)

	buf := make([]model.StepVector, 1)
	n, err := op.Next(context.Background(), buf)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// Exactly one CountSeries call, with the full lookback window — NOT clamped to [T, T].
	require.Len(t, fc.calls, 1, "instant count must issue one CountSeries call")
	wantStart := evalT.UnixMilli() - lookback.Milliseconds()
	wantEnd := evalT.UnixMilli()
	assert.Equal(t, window{wantStart, wantEnd}, fc.calls[0],
		"lookback window must extend before the evaluation time; clamping to mint breaks instant count")

	// The single emitted sample carries the count over the empty label set (series id 0).
	require.Len(t, buf[0].Samples, 1)
	assert.Equal(t, float64(7), buf[0].Samples[0])
	assert.Equal(t, evalT.UnixMilli(), buf[0].T)
}

// TestCountSelectorRangeLookbackWindow verifies the per-step window for a range count: each step at
// T_i asks for [T_i - lookback, T_i], including the first step whose window precedes the range start
// (matches Prometheus instant-vector semantics within a range evaluation).
func TestCountSelectorRangeLookbackWindow(t *testing.T) {
	const (
		lookback = 5 * time.Minute
		step     = time.Minute
	)
	start := time.Unix(10_000, 0)
	end := start.Add(2 * step) // 3 steps
	opts := &query.Options{
		Start:         start,
		End:           end,
		Step:          step,
		LookbackDelta: lookback,
	}

	fc := &fakeCounter{count: 3}
	op := NewCountSelector(fc, nil, opts)

	buf := make([]model.StepVector, 8)
	n, err := op.Next(context.Background(), buf)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Len(t, fc.calls, 3)

	for i, c := range fc.calls {
		ts := start.Add(time.Duration(i) * step).UnixMilli()
		assert.Equal(t, window{ts - lookback.Milliseconds(), ts}, c,
			"step %d window must be [T-lookback, T], not clamped to the range start", i)
	}
}

// TestCountSelectorEmptySelectorSkips verifies the pushdown matches Prometheus aggregation
// semantics for a genuinely-empty selector: count() over no matched series emits no sample (the
// empty vector), not {0}. A step with zero matched series is a gap, so the single output series
// never accrues a sample and the matrix comes out empty.
func TestCountSelectorEmptySelectorSkips(t *testing.T) {
	opts := &query.Options{
		Start:         time.Unix(1, 0),
		End:           time.Unix(1, 0),
		Step:          0,
		LookbackDelta: 5 * time.Minute,
	}
	fc := &fakeCounter{count: 0} // no series match
	op := NewCountSelector(fc, []*labels.Matcher{}, opts)

	buf := make([]model.StepVector, 1)
	n, err := op.Next(context.Background(), buf)
	require.NoError(t, err)
	require.Equal(t, 0, n, "count() over an empty selector must emit no sample (gap, not {0})")
}
