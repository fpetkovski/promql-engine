// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package binary

import (
	"context"
	"fmt"
	"github.com/efficientgo/core/errors"
	"github.com/thanos-community/promql-engine/physicalplan/parse"
	"gonum.org/v1/gonum/floats"
	"math"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-community/promql-engine/physicalplan/model"
)

// scalarOperator evaluates expressions where one operand is a scalarOperator.
type scalarOperator struct {
	seriesOnce sync.Once
	series     []labels.Labels
	scalar     float64

	numberSelector model.VectorOperator
	next           model.VectorOperator
	operation      vectorizedOperation
}

func NewScalar(next model.VectorOperator, numberSelector model.VectorOperator, op parser.ItemType, scalarSideLeft bool) (*scalarOperator, error) {
	ops := scalarLeftOperators
	if !scalarSideLeft {
		ops = scalarRightOperators
	}
	vectorizedOperation, err := getVectorizedOperation(op, ops)
	if err != nil {
		return nil, err
	}

	// Cache the result of the number selector since it
	// will not change during execution.
	v, err := numberSelector.Next(context.Background())
	if err != nil {
		return nil, err
	}
	scalar := v[0].Samples[0]

	return &scalarOperator{
		next:           next,
		scalar:         scalar,
		numberSelector: numberSelector,
		operation:      vectorizedOperation,
	}, nil
}

func (o *scalarOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	var err error
	o.seriesOnce.Do(func() { err = o.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}
	return o.series, nil
}

func (o *scalarOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	in, err := o.next.Next(ctx)
	if err != nil {
		return nil, err
	}
	if in == nil {
		return nil, nil
	}
	o.seriesOnce.Do(func() { err = o.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}

	for _, vector := range in {
		o.operation(o.scalar, vector.Samples)
	}
	return in, nil
}

func (o *scalarOperator) GetPool() *model.VectorPool {
	return o.next.GetPool()
}

func (o *scalarOperator) loadSeries(ctx context.Context) error {
	vectorSeries, err := o.next.Series(ctx)
	if err != nil {
		return err
	}
	series := make([]labels.Labels, len(vectorSeries))
	for i := range vectorSeries {
		lbls := labels.NewBuilder(vectorSeries[i]).Del(labels.MetricName).Labels()
		series[i] = lbls
	}

	o.series = series

	return nil
}

type vectorizedOperation func(float64, []float64)

var scalarRightOperators = map[int]vectorizedOperation{
	parser.ADD: floats.AddConst,
	parser.SUB: func(f float64, vector []float64) { floats.AddConst(-f, vector) },
	parser.MUL: floats.Scale,
	parser.DIV: func(f float64, vector []float64) { floats.Scale(1/f, vector) },
	parser.POW: func(f float64, vector []float64) {
		for i := range vector {
			vector[i] = math.Pow(vector[i], f)
		}
	},
	parser.MOD: func(f float64, vector []float64) {
		for i := range vector {
			vector[i] = float64(int64(vector[i]) % (int64(f)))
		}
	},
	parser.AT: func(f float64, vector []float64) {
		for i := range vector {
			vector[i] = math.Atan2(vector[i], f)
		}
	},
}

var scalarLeftOperators = map[int]vectorizedOperation{
	parser.ADD: floats.AddConst,
	parser.SUB: func(f float64, vector []float64) {
		floats.Scale(-1, vector)
		floats.AddConst(f, vector)
	},
	parser.MUL: floats.Scale,
	parser.DIV: func(f float64, vector []float64) {
		for i := range vector {
			vector[i] = f / vector[i]
		}
	},
	parser.POW: func(f float64, vector []float64) {
		for i := range vector {
			vector[i] = math.Pow(f, vector[i])
		}
	},
	parser.MOD: func(f float64, vector []float64) {
		for i := range vector {
			vector[i] = math.Mod(f, vector[i])
		}
	},
	parser.ATAN2: func(f float64, vector []float64) {
		for i := range vector {
			vector[i] = math.Atan2(f, vector[i])
		}
	},
}

func getVectorizedOperation(expr parser.ItemType, ops map[int]vectorizedOperation) (vectorizedOperation, error) {
	if o, ok := ops[int(expr)]; ok {
		return o, nil
	}
	t := parser.ItemTypeStr[expr]
	msg := fmt.Sprintf("operation not supported: %s", t)
	return nil, errors.Wrap(parse.ErrNotSupportedExpr, msg)
}
