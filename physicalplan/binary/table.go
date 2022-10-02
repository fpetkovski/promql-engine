// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package binary

import (
	"fmt"
	"github.com/efficientgo/core/errors"
	"math"

	"github.com/thanos-community/promql-engine/physicalplan/parse"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-community/promql-engine/physicalplan/model"
)

type sample struct {
	t int64
	v float64
}

type table struct {
	pool *model.VectorPool

	operation operation
	card      parser.VectorMatchCardinality

	outputValues []sample
	// highCardOutputIndex is a mapping from series ID of the high cardinality
	// operator to an output series ID.
	// During joins, each high cardinality series that has a matching
	// low cardinality series will map to exactly one output series.
	highCardOutputIndex outputIndex
	// lowCardOutputIndex is a mapping from series ID of the low cardinality
	// operator to an output series ID.
	// Each series from the low cardinality operator can join with many
	// series of the high cardinality operator.
	lowCardOutputIndex outputIndex
}

func newTable(
	pool *model.VectorPool,
	card parser.VectorMatchCardinality,
	operation operation,
	outputValues []sample,
	highCardOutputCache outputIndex,
	lowCardOutputCache outputIndex,
) *table {
	return &table{
		pool: pool,
		card: card,

		operation:           operation,
		outputValues:        outputValues,
		highCardOutputIndex: highCardOutputCache,
		lowCardOutputIndex:  lowCardOutputCache,
	}
}

func (t *table) execBinaryOperation(lhs model.StepVector, rhs model.StepVector) model.StepVector {
	ts := lhs.T
	step := t.pool.GetStepVector(ts)

	lhsIndex, rhsIndex := t.highCardOutputIndex, t.lowCardOutputIndex
	if t.card == parser.CardOneToMany {
		lhsIndex, rhsIndex = rhsIndex, lhsIndex
	}

	for i, sampleID := range lhs.SampleIDs {
		lhsVal := lhs.Samples[i]
		outputSampleIDs := lhsIndex.outputSamples(sampleID)
		for _, outputSampleID := range outputSampleIDs {
			t.outputValues[outputSampleID].t = lhs.T
			t.outputValues[outputSampleID].v = lhsVal
		}
	}

	for i, sampleID := range rhs.SampleIDs {
		rhVal := rhs.Samples[i]
		outputSampleIDs := rhsIndex.outputSamples(sampleID)
		for _, outputSampleID := range outputSampleIDs {
			lhSample := t.outputValues[outputSampleID]
			if rhs.T != lhSample.t {
				continue
			}

			outputVal := t.operation(lhSample.v, rhVal)
			step.SampleIDs = append(step.SampleIDs, outputSampleID)
			step.Samples = append(step.Samples, outputVal)
		}
	}

	return step
}

type operation func(lhs, rhs float64) float64

var operations = map[int]operation{
	parser.ADD:   func(lhs, rhs float64) float64 { return lhs + rhs },
	parser.SUB:   func(lhs, rhs float64) float64 { return lhs - rhs },
	parser.MUL:   func(lhs, rhs float64) float64 { return lhs * rhs },
	parser.DIV:   func(lhs, rhs float64) float64 { return lhs / rhs },
	parser.POW:   func(lhs, rhs float64) float64 { return math.Pow(lhs, rhs) },
	parser.MOD:   func(lhs, rhs float64) float64 { return math.Mod(lhs, rhs) },
	parser.ATAN2: func(lhs, rhs float64) float64 { return math.Atan2(lhs, rhs) },
}

func newOperation(expr parser.ItemType) (operation, error) {
	if o, ok := operations[int(expr)]; ok {
		return o, nil
	}

	t := parser.ItemTypeStr[expr]
	msg := fmt.Sprintf("operation not supported: %s", t)
	return nil, errors.Wrap(parse.ErrNotSupportedExpr, msg)
}
