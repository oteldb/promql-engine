// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"math"

	"github.com/oteldb/promql-engine/execution/exchange"
	"github.com/oteldb/promql-engine/execution/model"
	"github.com/oteldb/promql-engine/execution/telemetry"
	"github.com/oteldb/promql-engine/query"

	"github.com/prometheus/prometheus/model/labels"
)

type scalarOperator struct {
	pool *model.VectorPool
	next model.VectorOperator
}

func newScalarOperator(pool *model.VectorPool, next model.VectorOperator, opts *query.Options) model.VectorOperator {
	var op model.VectorOperator = &scalarOperator{
		pool: pool,
		next: next,
	}
	op = telemetry.NewOperator(telemetry.NewTelemetry(op, opts), op)
	op = exchange.NewConcurrent(op, 2, opts)
	return op
}

func (o *scalarOperator) String() string {
	return "[scalar]"
}

func (o *scalarOperator) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{o.next}
}

func (o *scalarOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	return nil, nil
}

func (o *scalarOperator) GetPool() *model.VectorPool {
	return o.pool
}

func (o *scalarOperator) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	n, err := o.next.Next(ctx, buf)
	if err != nil {
		return 0, err
	}

	result := o.GetPool().GetVectorBatch()
	for _, vector := range in {
		sv := o.GetPool().GetStepVector(vector.T)
		if len(vector.Samples) != 1 {
			sv.AppendSample(o.GetPool(), 0, math.NaN())
		} else {
			sv.AppendSample(o.GetPool(), 0, vector.Samples[0])
		}
		result = append(result, sv)
		o.next.GetPool().PutStepVector(vector)
	}
	o.next.GetPool().PutVectors(in)

	return n, nil
}
