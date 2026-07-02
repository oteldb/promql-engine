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

// fakeGroupedCounter records every CountSeriesBy window and returns fixed per-group counts.
type fakeGroupedCounter struct {
	counts map[string]uint64
	label  string
	calls  []window
}

func (f *fakeGroupedCounter) CountSeriesBy(
	_ context.Context, startMs, endMs int64, label string, _ ...*labels.Matcher,
) (map[string]uint64, error) {
	f.label = label
	f.calls = append(f.calls, window{startMs, endMs})

	return f.counts, nil
}

// TestCountBySelectorInstant pins the instant-query behavior: one output series per label value
// (sorted; the "" group is the empty label set), the full lookback window, and — because the
// enumeration window equals the single step's window — exactly ONE CountSeriesBy call total.
func TestCountBySelectorInstant(t *testing.T) {
	const lookback = 5 * time.Minute
	evalT := time.Unix(1000, 0)
	opts := &query.Options{
		Start:         evalT,
		End:           evalT,
		Step:          0, // instant
		LookbackDelta: lookback,
	}

	fc := &fakeGroupedCounter{counts: map[string]uint64{"1": 3, "0": 2, "": 1}}
	op := NewCountBySelector(fc, "cpu", nil, opts)

	series, err := op.Series(context.Background())
	require.NoError(t, err)
	require.Equal(t, []labels.Labels{
		labels.EmptyLabels(), // "" group: series without the label
		labels.FromStrings("cpu", "0"),
		labels.FromStrings("cpu", "1"),
	}, series)
	assert.Equal(t, "cpu", fc.label, "the grouping label reaches the counter")

	buf := make([]model.StepVector, 1)
	n, err := op.Next(context.Background(), buf)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// One call for the whole instant query: the enumeration doubles as the step's counts.
	require.Len(t, fc.calls, 1, "instant count by must issue one CountSeriesBy call")
	wantStart := evalT.UnixMilli() - lookback.Milliseconds()
	assert.Equal(t, window{wantStart, evalT.UnixMilli()}, fc.calls[0],
		"lookback window must extend before the evaluation time")

	// Samples in series order: "" ⇒ 1, cpu=0 ⇒ 2, cpu=1 ⇒ 3.
	require.Equal(t, []uint64{0, 1, 2}, buf[0].SampleIDs)
	assert.Equal(t, []float64{1, 2, 3}, buf[0].Samples)
	assert.Equal(t, evalT.UnixMilli(), buf[0].T)

	// Exhausted after the single step.
	n, err = op.Next(context.Background(), buf)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

// TestCountBySelectorRange verifies the per-step windows of a range query: one enumeration call
// over [mint-lookback, maxt], then one call per step at [T_i - lookback, T_i].
func TestCountBySelectorRange(t *testing.T) {
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

	fc := &fakeGroupedCounter{counts: map[string]uint64{"a": 4}}
	op := NewCountBySelector(fc, "zone", nil, opts)

	buf := make([]model.StepVector, 8)
	n, err := op.Next(context.Background(), buf)
	require.NoError(t, err)
	require.Equal(t, 3, n)

	require.Len(t, fc.calls, 4, "one enumeration + one call per step")
	assert.Equal(t, window{start.UnixMilli() - lookback.Milliseconds(), end.UnixMilli()}, fc.calls[0],
		"enumeration spans the whole range plus lookback")

	for i := range 3 {
		ts := start.Add(time.Duration(i) * step).UnixMilli()
		assert.Equal(t, window{ts - lookback.Milliseconds(), ts}, fc.calls[i+1], "step %d window", i)
		assert.Equal(t, []float64{4}, buf[i].Samples)
		assert.Equal(t, ts, buf[i].T)
	}
}

// TestCountBySelectorEmpty pins the empty-result semantics: no matched series over the whole range
// means no output series and no samples (the empty vector, matching count by over nothing).
func TestCountBySelectorEmpty(t *testing.T) {
	evalT := time.Unix(1000, 0)
	opts := &query.Options{
		Start:         evalT,
		End:           evalT,
		LookbackDelta: 5 * time.Minute,
	}

	fc := &fakeGroupedCounter{counts: map[string]uint64{}}
	op := NewCountBySelector(fc, "cpu", nil, opts)

	series, err := op.Series(context.Background())
	require.NoError(t, err)
	assert.Empty(t, series)

	buf := make([]model.StepVector, 1)
	n, err := op.Next(context.Background(), buf)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}
