// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package aggregate

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/oteldb/promql-engine/execution/model"
	"github.com/oteldb/promql-engine/query"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type gridCounter struct{ counts []uint64 }

func (c gridCounter) CountSeries(_ context.Context, _, end int64, _ ...*labels.Matcher) (uint64, error) {
	return c.counts[end/1000], nil
}

func (c gridCounter) CountSeriesBy(_ context.Context, start, end int64, _ string, _ ...*labels.Matcher) (map[string]uint64, error) {
	var count uint64
	for _, n := range c.counts {
		count = max(count, n)
	}
	if end-start <= 1 {
		count = c.counts[end/1000]
	}
	if count == 0 {
		return nil, nil
	}
	return map[string]uint64{"g": count}, nil
}

func TestCountPushdownKeepsEmptySteps(t *testing.T) {
	t.Parallel()
	for _, grouped := range []bool{false, true} {
		for _, size := range []int{1, 2, 4, 8} {
			t.Run(strconv.FormatBool(grouped)+"/"+strconv.Itoa(size), func(t *testing.T) {
				t.Parallel()
				for _, counts := range [][]uint64{{0, 2, 0, 0, 3, 0}, {0, 0, 0, 0, 0, 0}} {
					opts := &query.Options{Start: time.Unix(0, 0), End: time.Unix(5, 0), Step: time.Second, LookbackDelta: time.Millisecond}
					counter := gridCounter{counts: counts}
					var op model.VectorOperator
					if grouped {
						op = NewCountBySelector(counter, "group", nil, opts)
					} else {
						op = NewCountSelector(counter, nil, opts)
					}
					_, err := op.Series(t.Context())
					require.NoError(t, err)
					buf := make([]model.StepVector, size)
					for i := range buf {
						buf[i].AppendSample(99, 99)
					}
					next := 0
					for {
						n, err := op.Next(t.Context(), buf)
						require.NoError(t, err)
						if n == 0 {
							break
						}
						for _, v := range buf[:n] {
							require.Less(t, next, len(counts))
							assert.Equal(t, int64(next*1000), v.T)
							if counts[next] == 0 {
								assert.Empty(t, v.Samples)
								assert.Empty(t, v.SampleIDs)
							} else {
								assert.Equal(t, []float64{float64(counts[next])}, v.Samples)
								assert.Equal(t, []uint64{0}, v.SampleIDs)
							}
							next++
						}
					}
					assert.Equal(t, len(counts), next, "empty steps must not compress the evaluation grid")
				}
			})
		}
	}
}
