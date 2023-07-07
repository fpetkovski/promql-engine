// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package unary

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"gonum.org/v1/gonum/floats"

	"github.com/thanos-io/promql-engine/execution/model"
)

type unaryNegation struct {
	next model.VectorOperator
	model.OperatorTelemetry
}

func (u *unaryNegation) Explain() (me string, next []model.VectorOperator) {
	return "[*unaryNegation]", []model.VectorOperator{u.next}
}

func NewUnaryNegation(next model.VectorOperator) (model.VectorOperator, error) {
	u := &unaryNegation{
		next:              next,
		OperatorTelemetry: &model.TrackedTelemetry{},
	}

	return u, nil
}

func (u *unaryNegation) Analyze() (model.OperatorTelemetry, []model.ObservableVectorOperator) {
	u.SetName("[*unaryNegation]")
	next := make([]model.ObservableVectorOperator, 0, 1)
	if obsnext, ok := u.next.(model.ObservableVectorOperator); ok {
		next = append(next, obsnext)
	}
	return u, next
}

func (u *unaryNegation) Series(ctx context.Context) model.LabelsIterator {
	return model.NewProcessingIterator(u.next.Series(ctx), func(l labels.Labels) labels.Labels {
		return labels.NewBuilder(l).Del(labels.MetricName).Labels()
	})
}

func (u *unaryNegation) GetPool() *model.VectorPool {
	return u.next.GetPool()
}

func (u *unaryNegation) Next(ctx context.Context) ([]model.StepVector, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	start := time.Now()
	in, err := u.next.Next(ctx)
	if err != nil {
		return nil, err
	}
	if in == nil {
		return nil, nil
	}
	for i := range in {
		floats.Scale(-1, in[i].Samples)
	}
	u.AddExecutionTimeTaken(time.Since(start))
	return in, nil
}
