// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/oteldb/promql-engine/engine"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

func BenchmarkRangeCountProjection(b *testing.B) {
	const points = 1441
	base := time.Unix(1_000_000, 0)
	ts := make([]int64, points)
	values := make([]float64, points)
	for i := range ts {
		ts[i] = base.Add(time.Duration(i) * 30 * time.Second).UnixMilli()
		values[i] = float64(i)
	}
	series := make([]storage.Series, 0, 128)
	for cpu := range 16 {
		for _, mode := range []string{"user", "system", "idle", "iowait", "irq", "softirq", "nice", "steal"} {
			series = append(series, &mockSeries{timestamps: ts, values: values, labels: []string{"__name__", "cpu_total", "cpu", strconv.Itoa(cpu), "mode", mode}})
		}
	}
	source := &rangeCountQueryable{Queryable: storageWithSeries(series...), series: series, timestamps: ts}
	eng := engine.New(engine.Opts{EngineOpts: promql.EngineOpts{Timeout: time.Minute, MaxSamples: 10_000_000}, DecodingConcurrency: 1})
	const expr = `sum(cpu_total) / scalar(count(count by(cpu)(cpu_total)))`
	var lastPoints int
	b.ReportAllocs()
	for b.Loop() {
		q, err := eng.NewRangeQuery(b.Context(), source, nil, expr, base.Add(-12*time.Hour), base.Add(12*time.Hour), time.Minute)
		if err != nil {
			b.Fatal(err)
		}
		result := q.Exec(b.Context())
		if result.Err != nil {
			q.Close()
			b.Fatal(result.Err)
		}
		matrix := result.Value.(promql.Matrix)
		lastPoints = 0
		for _, s := range matrix {
			lastPoints += len(s.Floats)
		}
		q.Close()
	}
	b.ReportMetric(float64(lastPoints), "points")
	b.ReportMetric(float64(source.countCalls+source.groupCalls)/float64(b.N), "count_calls/op")
}
