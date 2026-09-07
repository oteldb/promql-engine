// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package prometheus

import (
	"context"
	"sync"

	"github.com/oteldb/promql-engine/warnings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

type SeriesSelector interface {
	GetSeries(ctx context.Context, shard, numShards int) ([]SignedSeries, error)
	Matchers() []*labels.Matcher
	// QuerierMu returns the lock serializing access to the shared querier.
	// Selectors that are not backed by one return [NoopLocker].
	QuerierMu() sync.Locker
}

// NoopLocker is a [sync.Locker] for selectors that do not share a prometheus
// querier and therefore need no serialization.
type NoopLocker struct{}

func (NoopLocker) Lock()   {}
func (NoopLocker) Unlock() {}

type SignedSeries struct {
	storage.Series
	Signature uint64
}

type seriesSelector struct {
	storage  storage.Querier
	matchers []*labels.Matcher
	hints    storage.SelectHints

	// querierMu is shared by all selectors backed by the same querier; it
	// serializes Select and chunk reads, neither of which is concurrency-safe on
	// a single querier: since prometheus v0.312 headIndexReader.Series mutates a
	// reusable buffer, and headChunkReader caches head chunks per reader.
	querierMu *sync.Mutex

	once   sync.Once
	series []SignedSeries
}

func newSeriesSelector(storage storage.Querier, querierMu *sync.Mutex, matchers []*labels.Matcher, hints storage.SelectHints) *seriesSelector {
	return &seriesSelector{
		storage:   storage,
		querierMu: querierMu,
		matchers:  matchers,
		hints:     hints,
	}
}

func (o *seriesSelector) QuerierMu() sync.Locker {
	return o.querierMu
}

func (o *seriesSelector) Matchers() []*labels.Matcher {
	return o.matchers
}

func (o *seriesSelector) GetSeries(ctx context.Context, shard int, numShards int) ([]SignedSeries, error) {
	var err error
	o.once.Do(func() { err = o.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}

	return seriesShard(o.series, shard, numShards), nil
}

func (o *seriesSelector) loadSeries(ctx context.Context) error {
	o.querierMu.Lock()
	defer o.querierMu.Unlock()

	seriesSet := o.storage.Select(ctx, false, &o.hints, o.matchers...)
	i := 0
	for seriesSet.Next() {
		s := seriesSet.At()
		o.series = append(o.series, SignedSeries{
			Series:    s,
			Signature: uint64(i),
		})
		i++
	}

	for _, w := range seriesSet.Warnings() {
		warnings.AddToContext(w, ctx)
	}
	return seriesSet.Err()
}

func seriesShard(series []SignedSeries, index int, numShards int) []SignedSeries {
	start := index * len(series) / numShards
	end := min((index+1)*len(series)/numShards, len(series))

	slice := series[start:end]
	shard := make([]SignedSeries, len(slice))
	copy(shard, slice)

	for i := range shard {
		shard[i].Signature = uint64(i)
	}
	return shard
}
