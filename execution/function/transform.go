// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"sync"

	"github.com/thanos-io/promql-engine/execution/model"

	"github.com/prometheus/prometheus/model/labels"
)

// transformOperator is a generic operator that applies transformations to series and/or samples.
// It consolidates common boilerplate code shared by operators like timestampOperator and relabelOperator.
type transformOperator struct {
	next   model.VectorOperator
	series []labels.Labels
	once   sync.Once

	// name is used for String() method
	name string

	// seriesTransform transforms the input series labels.
	// If nil, series are loaded from next operator without transformation.
	seriesTransform func(context.Context, []labels.Labels) ([]labels.Labels, error)

	// samplesTransform transforms the input sample vectors.
	// If nil, samples are passed through unchanged.
	samplesTransform func([]model.StepVector) ([]model.StepVector, error)
}

func (o *transformOperator) Explain() []model.VectorOperator {
	return []model.VectorOperator{o.next}
}

func (o *transformOperator) String() string {
	return "[" + o.name + "]"
}

func (o *transformOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	var err error
	o.once.Do(func() { err = o.loadSeries(ctx) })
	return o.series, err
}

func (o *transformOperator) GetPool() *model.VectorPool {
	return o.next.GetPool()
}

func (o *transformOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	in, err := o.next.Next(ctx)
	if err != nil {
		return nil, err
	}

	if o.samplesTransform != nil {
		return o.samplesTransform(in)
	}

	return in, nil
}

func (o *transformOperator) loadSeries(ctx context.Context) error {
	series, err := o.next.Series(ctx)
	if err != nil {
		return err
	}

	if o.seriesTransform != nil {
		o.series, err = o.seriesTransform(ctx, series)
		return err
	}

	o.series = series
	return nil
}
