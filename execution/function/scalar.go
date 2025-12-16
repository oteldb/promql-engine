// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"math"
	"sync"

	"github.com/oteldb/promql-engine/execution/model"
	"github.com/oteldb/promql-engine/execution/telemetry"
	"github.com/oteldb/promql-engine/query"

	"github.com/prometheus/prometheus/model/labels"
)

type scalarOperator struct {
	pool *model.VectorPool
	next model.VectorOperator
	once sync.Once
}

func newScalarOperator(pool *model.VectorPool, next model.VectorOperator, opts *query.Options) model.VectorOperator {
	oper := &scalarOperator{
		pool: pool,
		next: next,
	}
	return telemetry.NewOperator(telemetry.NewTelemetry(oper, opts), oper)
}

func (o *scalarOperator) String() string {
	return "[scalar]"
}

func (o *scalarOperator) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{o.next}
}

func (o *scalarOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	err := o.init(ctx)
	return nil, err
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

func (o *scalarOperator) init(ctx context.Context) (err error) {
	o.once.Do(func() {
		_, err = o.next.Series(ctx)
	})
	return err
}
