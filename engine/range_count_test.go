// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/oteldb/promql-engine/engine"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type rangeCountQueryable struct {
	storage.Queryable
	series     []storage.Series
	timestamps []int64
	countCalls int
	groupCalls int
}

type rangeCountQuerier struct {
	storage.Querier
	owner *rangeCountQueryable
}

func (q *rangeCountQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	querier, err := q.Queryable.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &rangeCountQuerier{Querier: querier, owner: q}, nil
}

func (q *rangeCountQuerier) active(start, end int64) bool {
	for _, ts := range q.owner.timestamps {
		if ts >= start && ts <= end {
			return true
		}
	}
	return false
}

func (q *rangeCountQuerier) CountSeries(_ context.Context, start, end int64, matchers ...*labels.Matcher) (uint64, error) {
	q.owner.countCalls++
	if !q.active(start, end) {
		return 0, nil
	}
	var n uint64
	for _, s := range q.owner.series {
		if rangeCountMatches(s.Labels(), matchers) {
			n++
		}
	}
	return n, nil
}

func (q *rangeCountQuerier) CountSeriesBy(_ context.Context, start, end int64, label string, matchers ...*labels.Matcher) (map[string]uint64, error) {
	q.owner.groupCalls++
	groups := map[string]uint64{}
	if q.active(start, end) {
		for _, s := range q.owner.series {
			if rangeCountMatches(s.Labels(), matchers) {
				groups[s.Labels().Get(label)]++
			}
		}
	}
	return groups, nil
}

func rangeCountMatches(set labels.Labels, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(set.Get(m.Name)) {
			return false
		}
	}
	return true
}

func rangeCountFixture() *rangeCountQueryable {
	ts := []int64{120000, 180000, 240000, 600000, 660000, 720000, 780000}
	vals := []float64{1, 2, 3, 4, 5, 6, 7}
	series := []storage.Series{
		&mockSeries{timestamps: ts, values: vals, labels: []string{"__name__", "cpu_total", "cpu", "0"}},
		&mockSeries{timestamps: ts, values: vals, labels: []string{"__name__", "cpu_total", "cpu", "1"}},
	}
	return &rangeCountQueryable{Queryable: storageWithSeries(series...), series: series, timestamps: ts}
}

func TestRangeCountMatchesPrometheusWithGaps(t *testing.T) {
	t.Parallel()
	for _, expr := range []string{
		`count(cpu_total)`,
		`count by(cpu)(cpu_total)`,
		`sum(cpu_total) / scalar(count(cpu_total))`,
		`sum(cpu_total) / scalar(count(count by(cpu)(cpu_total)))`,
		`scalar(count(count by(cpu)(cpu_total))) + sum(cpu_total)`,
		`sum(cpu_total) / scalar(count by(cpu)(cpu_total{cpu="0"}))`,
		`scalar(count(missing))`,
	} {
		t.Run(expr, func(t *testing.T) {
			t.Parallel()
			source := rangeCountFixture()
			opts := promql.EngineOpts{Timeout: time.Minute, MaxSamples: 1e6, LookbackDelta: time.Minute}
			ctx := t.Context()
			start, end := time.Unix(0, 0), time.Unix(15*60, 0)
			reference := newPromEngine(opts)
			rq, err := reference.NewRangeQuery(ctx, source.Queryable, nil, expr, start, end, time.Minute)
			require.NoError(t, err)
			defer rq.Close()
			want := rq.Exec(ctx)
			require.NoError(t, want.Err)
			actual := engine.New(engine.Opts{EngineOpts: opts, DecodingConcurrency: 1})
			aq, err := actual.NewRangeQuery(ctx, source, nil, expr, start, end, time.Minute)
			require.NoError(t, err)
			defer aq.Close()
			got := aq.Exec(ctx)
			require.NoError(t, got.Err)
			require.Len(t, got.Value.(promql.Matrix), len(want.Value.(promql.Matrix)))
			for i, s := range want.Value.(promql.Matrix) {
				result := got.Value.(promql.Matrix)[i]
				assert.Equal(t, s.Metric, result.Metric)
				require.Len(t, result.Floats, len(s.Floats), "empty steps must not truncate or shift a binary expression")
				for j, p := range s.Floats {
					assert.Equal(t, p.T, result.Floats[j].T)
					if math.IsNaN(p.F) {
						assert.True(t, math.IsNaN(result.Floats[j].F))
					} else {
						assert.Equal(t, p.F, result.Floats[j].F)
					}
				}
			}
			assert.Zero(t, source.countCalls, "range count must reuse the materialized selector and grouping projection")
			assert.Zero(t, source.groupCalls)
		})
	}
}

func TestInstantCountKeepsPushdown(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		expr    string
		grouped bool
	}{
		{`count(cpu_total)`, false},
		{`count(count by(cpu)(cpu_total))`, true},
	} {
		t.Run(tc.expr, func(t *testing.T) {
			t.Parallel()
			source := rangeCountFixture()
			actual := engine.New(engine.Opts{EngineOpts: promql.EngineOpts{Timeout: time.Minute, MaxSamples: 1e6, LookbackDelta: time.Minute}, DecodingConcurrency: 1})
			q, err := actual.NewInstantQuery(t.Context(), source, nil, tc.expr, time.Unix(12*60, 0))
			require.NoError(t, err)
			defer q.Close()
			result := q.Exec(t.Context())
			require.NoError(t, result.Err)
			values := result.Value.(promql.Vector)
			require.Len(t, values, 1)
			assert.Equal(t, float64(2), values[0].F)
			if tc.grouped {
				assert.Equal(t, 1, source.groupCalls)
				assert.Zero(t, source.countCalls)
			} else {
				assert.Equal(t, 1, source.countCalls)
				assert.Zero(t, source.groupCalls)
			}
		})
	}
}
