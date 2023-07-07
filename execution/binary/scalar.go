// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package binary

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/parser"
	"github.com/thanos-io/promql-engine/query"
)

type ScalarSide int

const (
	ScalarSideBoth ScalarSide = iota
	ScalarSideLeft
	ScalarSideRight
)

// scalarOperator evaluates expressions where one operand is a scalarOperator.
type scalarOperator struct {
	pool          *model.VectorPool
	scalar        model.VectorOperator
	next          model.VectorOperator
	opType        parser.ItemType
	getOperands   getOperandsFunc
	operandValIdx int
	floatOp       operation
	histOp        histogramFloatOperation

	// If true then return the comparison result as 0/1.
	returnBool bool

	// Keep the result if both sides are scalars.
	bothScalars bool
	model.OperatorTelemetry
}

func NewScalar(
	pool *model.VectorPool,
	next model.VectorOperator,
	scalar model.VectorOperator,
	op parser.ItemType,
	scalarSide ScalarSide,
	returnBool bool,
	opts *query.Options,
) (*scalarOperator, error) {
	binaryOperation, err := newOperation(op, scalarSide != ScalarSideBoth)
	if err != nil {
		return nil, err
	}
	// operandValIdx 0 means to get lhs as the return value
	// while 1 means to get rhs as the return value.
	operandValIdx := 0
	getOperands := getOperandsScalarRight
	if scalarSide == ScalarSideLeft {
		getOperands = getOperandsScalarLeft
		operandValIdx = 1
	}

	o := &scalarOperator{
		pool:          pool,
		next:          next,
		scalar:        scalar,
		floatOp:       binaryOperation,
		histOp:        getHistogramFloatOperation(op, scalarSide),
		opType:        op,
		getOperands:   getOperands,
		operandValIdx: operandValIdx,
		returnBool:    returnBool,
		bothScalars:   scalarSide == ScalarSideBoth,
	}
	o.OperatorTelemetry = &model.NoopTelemetry{}
	if opts.EnableAnalysis {
		o.OperatorTelemetry = &model.TrackedTelemetry{}
	}
	return o, nil

}

func (o *scalarOperator) Analyze() (model.OperatorTelemetry, []model.ObservableVectorOperator) {
	o.SetName("[*scalarOperator]")
	next := make([]model.ObservableVectorOperator, 0, 2)
	if obsnext, ok := o.next.(model.ObservableVectorOperator); ok {
		next = append(next, obsnext)
	}
	if obsnextScalar, ok := o.scalar.(model.ObservableVectorOperator); ok {
		next = append(next, obsnextScalar)
	}
	return o, next
}

func (o *scalarOperator) Explain() (me string, next []model.VectorOperator) {
	return fmt.Sprintf("[*scalarOperator] %s", parser.ItemTypeStr[o.opType]), []model.VectorOperator{o.next, o.scalar}
}

func (o *scalarOperator) Series(ctx context.Context) model.LabelsIterator {
	vectorSeries := o.next.Series(ctx)
	b := labels.ScratchBuilder{}
	return model.NewProcessingIterator(vectorSeries, func(lbls labels.Labels) labels.Labels {
		if !lbls.IsEmpty() {
			lbls := lbls
			if shouldDropMetricName(o.opType, o.returnBool) {
				lbls, _ = extlabels.DropMetricName(lbls, b)
			}
			return lbls
		} else {
			return lbls
		}
	})
}

func (o *scalarOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	start := time.Now()

	in, err := o.next.Next(ctx)
	if err != nil {
		return nil, err
	}
	if in == nil {
		return nil, nil
	}

	scalarIn, err := o.scalar.Next(ctx)
	if err != nil {
		return nil, err
	}

	out := o.pool.GetVectorBatch()
	for v, vector := range in {
		step := o.pool.GetStepVector(vector.T)
		scalarVal := math.NaN()
		if len(scalarIn) > v && len(scalarIn[v].Samples) > 0 {
			scalarVal = scalarIn[v].Samples[0]
		}

		for i := range vector.Samples {
			operands := o.getOperands(vector, i, scalarVal)
			val, keep := o.floatOp(operands, o.operandValIdx)
			if o.returnBool {
				if !o.bothScalars {
					val = 0.0
					if keep {
						val = 1.0
					}
				}
			} else if !keep {
				continue
			}
			step.AppendSample(o.pool, vector.SampleIDs[i], val)
		}

		for i := range vector.HistogramIDs {
			val := o.histOp(vector.Histograms[i], scalarVal)
			if val != nil {
				step.AppendHistogram(o.pool, vector.HistogramIDs[i], val)
			}
		}

		out = append(out, step)
		o.next.GetPool().PutStepVector(vector)
	}

	for i := range scalarIn {
		o.scalar.GetPool().PutStepVector(scalarIn[i])
	}

	o.next.GetPool().PutVectors(in)
	o.scalar.GetPool().PutVectors(scalarIn)
	o.AddExecutionTimeTaken(time.Since(start))

	return out, nil
}

func (o *scalarOperator) GetPool() *model.VectorPool {
	return o.pool
}

type getOperandsFunc func(v model.StepVector, i int, scalar float64) [2]float64

func getOperandsScalarLeft(v model.StepVector, i int, scalar float64) [2]float64 {
	return [2]float64{scalar, v.Samples[i]}
}

func getOperandsScalarRight(v model.StepVector, i int, scalar float64) [2]float64 {
	return [2]float64{v.Samples[i], scalar}
}
