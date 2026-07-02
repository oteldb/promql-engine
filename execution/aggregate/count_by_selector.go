// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package aggregate

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/oteldb/promql-engine/execution/model"
	"github.com/oteldb/promql-engine/execution/telemetry"
	"github.com/oteldb/promql-engine/query"
	"github.com/oteldb/promql-engine/storage"

	"github.com/prometheus/prometheus/model/labels"
)

// countBySelector is the count by (label) pushdown: it answers an instant or range
// `count by (L)(<vector selector>)` (single grouping label, no `without`, no offset/`@`) from the
// storage's [GroupedSeriesCounter] directly, instead of selecting every matched series' samples
// and labels and then grouping them. The result is one series per distinct value of L (series
// without the label group under the empty label set), each carrying the group's series count per
// step — matching the generic aggregation-over-Select path at a fraction of the cost for
// high-cardinality selectors. The enclosing `count(count by (L)(...))` (distinct label values)
// composes on top through the generic outer aggregation.
//
// The output series set must be fixed before the first step, so the operator first enumerates the
// label's values over the whole query range (one grouped count over [mint-lookback, maxt]); each
// step then recomputes its own lookback-window counts, emitting samples only for the groups
// present at that step. For an instant query the enumeration window equals the single step's
// window, so its counts are reused and the query costs one call.
type countBySelector struct {
	counter storage.GroupedSeriesCounter
	label   string
	matcher []*labels.Matcher

	opts       *query.Options
	lookback   int64 // ms
	mint, maxt int64 // ms
	step       int64 // ms

	currentStep int64
	series      []labels.Labels
	values      []string          // label value per output series, aligned with series
	firstCounts map[string]uint64 // enumeration counts, reused for an instant query's single step
	loadErr     error
	once        sync.Once
}

// NewCountBySelector returns an operator that evaluates count by (label)(<selector>) via counter.
func NewCountBySelector(
	counter storage.GroupedSeriesCounter, label string, matcher []*labels.Matcher, opts *query.Options,
) model.VectorOperator {
	o := &countBySelector{
		counter:  counter,
		label:    label,
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

	// See countSelector: an instant query (Step == 0) must still terminate after its single step.
	if o.step == 0 {
		o.step = 1
	}

	o.currentStep = o.mint

	return telemetry.NewOperator(telemetry.NewTelemetry(o, opts), o)
}

func (o *countBySelector) Explain() (next []model.VectorOperator) { return nil }

func (o *countBySelector) String() string {
	return fmt.Sprintf("[count-by-selector] by (%s) %d matcher(s)", o.label, len(o.matcher))
}

func (o *countBySelector) Series(ctx context.Context) ([]labels.Labels, error) {
	if err := o.loadSeries(ctx); err != nil {
		return nil, err
	}

	return o.series, nil
}

func (o *countBySelector) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	if o.currentStep > o.maxt {
		return 0, nil
	}

	if err := o.loadSeries(ctx); err != nil {
		return 0, err
	}

	n := 0
	for n < len(buf) && o.currentStep <= o.maxt {
		// Same window semantics as countSelector: a series counts toward its group at T if it has
		// a sample in [T-lookback, T] (unclamped; see the countSelector comment).
		start := o.currentStep - o.lookback

		// An instant query's single step shares the enumeration window; reuse those counts.
		counts := o.firstCounts
		o.firstCounts = nil

		if counts == nil {
			var err error

			counts, err = o.counter.CountSeriesBy(ctx, start, o.currentStep, o.label, o.matcher...)
			if err != nil {
				return n, err
			}
		}

		// Like count() over an empty vector, a step where no series matched emits no samples (a
		// gap); a group absent at this step simply contributes none.
		if len(counts) > 0 {
			buf[n].Reset(o.currentStep)

			// Emit in series order so sample slices stay ordered by series index.
			for i, v := range o.values {
				if c, ok := counts[v]; ok && c > 0 {
					buf[n].AppendSample(uint64(i), float64(c))
				}
			}

			n++
		}

		o.currentStep += o.step
	}

	return n, nil
}

// loadSeries enumerates the label's values over the whole query range and fixes the output series
// set: one series per value, labeled {label=value}; the "" group (series without the label) is the
// empty label set. Sorted by value for deterministic series order.
func (o *countBySelector) loadSeries(ctx context.Context) error {
	o.once.Do(func() {
		counts, err := o.counter.CountSeriesBy(ctx, o.mint-o.lookback, o.maxt, o.label, o.matcher...)
		if err != nil {
			o.loadErr = err

			return
		}

		values := make([]string, 0, len(counts))
		for v := range counts {
			values = append(values, v)
		}

		sort.Strings(values)

		o.series = make([]labels.Labels, len(values))
		o.values = values

		for i, v := range values {
			if v == "" {
				o.series[i] = labels.EmptyLabels()
			} else {
				o.series[i] = labels.FromStrings(o.label, v)
			}
		}

		// The single-step (instant) window equals the enumeration window; keep the counts so Next
		// answers without a second call.
		if o.mint == o.maxt {
			o.firstCounts = counts
		}
	})

	return o.loadErr
}
