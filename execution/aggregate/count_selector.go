// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package aggregate

import (
	"context"
	"fmt"
	"sync"

	"github.com/oteldb/promql-engine/execution/model"
	"github.com/oteldb/promql-engine/execution/telemetry"
	"github.com/oteldb/promql-engine/query"
	"github.com/oteldb/promql-engine/storage"

	"github.com/prometheus/prometheus/model/labels"
)

// countSelector is the count() pushdown: it answers an instant `count(<vector selector>)` (no
// `by`, no `without`, no offset/`@`) from the storage's [SeriesCounter] directly, instead of
// selecting every matched series' samples and labels and then counting them. The result is a
// single sample per step — a vector of one series (the empty label set) with the count value —
// matching what the generic aggregation-over-Select path would produce, at a fraction of the
// cost for high-cardinality selectors.
//
// For an instant query there is exactly one step; for a range query the count is recomputed per
// step over that step's lookback window. Each step is one cheap CountSeries call (no
// materialization), so a range count is also far cheaper than the materialize-then-count path.
type countSelector struct {
	counter storage.SeriesCounter
	matcher []*labels.Matcher

	opts    *query.Options
	lookback int64 // ms
	mint, maxt int64 // ms
	step    int64 // ms

	currentStep int64
	series      []labels.Labels
	once        sync.Once
}

// NewCountSelector returns an operator that evaluates count(<selector>) via counter.
func NewCountSelector(counter storage.SeriesCounter, matcher []*labels.Matcher, opts *query.Options) model.VectorOperator {
	o := &countSelector{
		counter:  counter,
		matcher:  matcher,
		opts:     opts,
		lookback: opts.LookbackDelta.Milliseconds(),
		mint:     opts.Start.UnixMilli(),
		maxt:     opts.End.UnixMilli(),
		step:     opts.Step.Milliseconds(),
	}

	if o.lookback <= 0 {
		o.lookback = 5 * 60 * 1000 // Prometheus default lookback delta.
	}

	// currentStep advances by o.step each Next; for an instant query (Step == 0) the operator must
	// still terminate after the single step, so a zero step is bumped to 1 ms (mirroring the
	// number-literal selector). Computing it as maxt-mint would leave it at 0 for an instant query
	// (mint == maxt) and loop forever.
	if o.step == 0 {
		o.step = 1
	}

	o.currentStep = o.mint

	return telemetry.NewOperator(telemetry.NewTelemetry(o, opts), o)
}

func (o *countSelector) Explain() (next []model.VectorOperator) { return nil }

func (o *countSelector) String() string {
	return fmt.Sprintf("[count-selector] %d matcher(s)", len(o.matcher))
}

func (o *countSelector) Series(context.Context) ([]labels.Labels, error) {
	o.loadSeries()

	return o.series, nil
}

func (o *countSelector) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	if o.currentStep > o.maxt {
		return 0, nil
	}

	o.loadSeries()

	n := 0
	for n < len(buf) && o.currentStep <= o.maxt {
		// count(selector) at T counts series with a sample in [T-lookback, T].
		start := o.currentStep - o.lookback
		if start < o.mint {
			start = o.mint
		}

		count, err := o.counter.CountSeries(ctx, start, o.currentStep, o.matcher...)
		if err != nil {
			return n, err
		}

		buf[n].Reset(o.currentStep)
		buf[n].AppendSample(0, float64(count))

		o.currentStep += o.step
		n++
	}

	return n, nil
}

func (o *countSelector) loadSeries() {
	o.once.Do(func() {
		// count() with no grouping emits one series — the empty label set.
		o.series = []labels.Labels{{}}
	})
}
