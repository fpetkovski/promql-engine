// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	"github.com/thanos-io/promql-engine/warnings"

	"github.com/cespare/xxhash/v2"
	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
)

type histogramOperation uint8

const (
	histogramQuantileOperation histogramOperation = iota
	histogramFractionOperation
)

type histogramSeries struct {
	outputID         int
	upperBound       float64
	hasBucketValue   bool
	bucketLabelValue string // original bucket label value for use in warnings
}

// quantilePoint is a quantile value at one evaluation step. Plural quantiles
// include the offset of the output-series block carrying the formatted value.
type quantilePoint struct {
	timestamp    int64
	value        float64
	outputOffset uint64
}

// histogramOperator reconstructs classic histograms and evaluates histogram
// functions over classic and native histograms. A singular histogram quantile
// is represented as a quantile operation with one scalar argument.
type histogramOperator struct {
	once      sync.Once
	seriesErr error

	operation histogramOperation
	funcName  string
	funcArgs  logicalplan.Nodes

	stepsBatch int
	nextOps    []model.VectorOperator
	vectorOp   model.VectorOperator
	scalarOps  []model.VectorOperator

	// addQuantileLabel distinguishes histogram_quantiles from the singular
	// function. The label name itself may be empty, so it cannot be the sentinel.
	addQuantileLabel bool
	quantileLabel    string

	// baseSeries contains one entry per reconstructed histogram. series is the
	// externally visible output and contains one block of baseSeries for each
	// quantile argument/value pair for histogram_quantiles.
	baseSeries      []labels.Labels
	baseSeriesNames []string
	series          []labels.Labels

	// scalarPoints contains one batch of scalar values for singular quantiles or
	// histogram_fraction. Plural quantiles are evaluated during Series because
	// their values are part of the output label sets.
	scalarPoints   [][]float64
	quantilePoints [][]quantilePoint
	quantileCursor int

	// outputIndex maps an input series ID to its reconstructed histogram ID and,
	// for float samples, the parsed le boundary.
	outputIndex []*histogramSeries

	// inputSeriesNames is needed for warnings tied to malformed input series.
	inputSeriesNames []string

	// seriesBuckets contains the classic buckets for each reconstructed
	// histogram at the current step.
	seriesBuckets []promql.Buckets

	// badBucketWarned tracks which series have already emitted bad bucket label warnings.
	badBucketWarned map[uint64]bool

	vectorBuf  []model.StepVector
	scalarBufs [][]model.StepVector
}

func newHistogramOperator(
	call *logicalplan.FunctionCall,
	nextOps []model.VectorOperator,
	stepsBatch int,
	opts *query.Options,
) model.VectorOperator {
	o := &histogramOperator{
		funcName:   call.Func.Name,
		funcArgs:   call.Args,
		stepsBatch: stepsBatch,
		nextOps:    nextOps,
	}

	// String arguments are omitted from nextOps, so normalize each public
	// signature into a vector input and an ordered list of scalar inputs.
	switch o.funcName {
	case "histogram_quantile":
		o.operation = histogramQuantileOperation
		o.scalarOps = nextOps[:1]
		o.vectorOp = nextOps[1]
	case "histogram_quantiles":
		o.operation = histogramQuantileOperation
		o.vectorOp = nextOps[0]
		o.scalarOps = nextOps[1:]
		o.addQuantileLabel = true
		o.quantileLabel = logicalplan.UnsafeUnwrapString(call.Args[1])
	case "histogram_fraction":
		o.operation = histogramFractionOperation
		o.scalarOps = nextOps[:2]
		o.vectorOp = nextOps[2]
	default:
		panic("unsupported function passed")
	}

	return telemetry.NewOperator(telemetry.NewTelemetry(o, opts), o)
}

func (o *histogramOperator) String() string {
	return fmt.Sprintf("[%s](%v)", o.funcName, o.funcArgs)
}

func (o *histogramOperator) Explain() []model.VectorOperator {
	return o.nextOps
}

func (o *histogramOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	o.once.Do(func() { o.seriesErr = o.loadSeries(ctx) })
	if o.seriesErr != nil {
		return nil, o.seriesErr
	}
	return o.series, nil
}

func (o *histogramOperator) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	o.once.Do(func() { o.seriesErr = o.loadSeries(ctx) })
	if o.seriesErr != nil {
		return 0, o.seriesErr
	}
	if len(buf) == 0 {
		return 0, nil
	}

	// Read vector data first to determine the number of steps in this batch.
	vectorBuf := o.vectorBuf
	if len(buf) < len(vectorBuf) {
		vectorBuf = vectorBuf[:len(buf)]
	}
	vectorN, err := o.vectorOp.Next(ctx, vectorBuf)
	if err != nil {
		return 0, err
	}
	if vectorN == 0 {
		return 0, nil
	}

	// Plural quantiles were consumed during Series so their label values could
	// be declared up front. Other operations continue to stream scalar inputs.
	if !o.addQuantileLabel {
		if err := o.readScalarPoints(ctx, vectorN); err != nil {
			return 0, err
		}
	}

	n, err := o.processInputSeries(ctx, vectorBuf[:vectorN], buf)
	if err != nil {
		return 0, err
	}
	if o.addQuantileLabel {
		o.quantileCursor += n
	}
	return n, nil
}

func (o *histogramOperator) readScalarPoints(ctx context.Context, numSteps int) error {
	for scalarIndex, scalarOp := range o.scalarOps {
		scalarN, err := scalarOp.Next(ctx, o.scalarBufs[scalarIndex][:numSteps])
		if err != nil {
			return err
		}

		for stepIndex := range numSteps {
			value := math.NaN()
			if stepIndex < scalarN && len(o.scalarBufs[scalarIndex][stepIndex].Samples) > 0 {
				value = o.scalarBufs[scalarIndex][stepIndex].Samples[0]
			}
			o.scalarPoints[scalarIndex][stepIndex] = value
			if o.operation == histogramQuantileOperation {
				o.warnInvalidQuantile(ctx, value)
			}
		}
	}
	return nil
}

func (o *histogramOperator) warnInvalidQuantile(ctx context.Context, value float64) {
	if math.IsNaN(value) || value < 0 || value > 1 {
		warnings.AddToContext(annotations.NewInvalidQuantileWarning(value, posrange.PositionRange{}), ctx)
	}
}

func (o *histogramOperator) processInputSeries(ctx context.Context, vectors []model.StepVector, buf []model.StepVector) (int, error) {
	n := 0
	for stepIndex, vector := range vectors {
		if n >= len(buf) {
			break
		}
		if err := o.validateQuantileStep(stepIndex, vector.T); err != nil {
			return 0, err
		}

		o.resetBuckets()
		for i, seriesID := range vector.SampleIDs {
			outputSeries := o.outputIndex[seriesID]
			// This means that it has an invalid `le` label.
			if outputSeries == nil || !outputSeries.hasBucketValue {
				// Emit warning for invalid bucket label only once per series.
				if outputSeries != nil && !o.badBucketWarned[seriesID] {
					o.badBucketWarned[seriesID] = true
					warnings.AddToContext(annotations.NewBadBucketLabelWarning(
						o.inputSeriesNames[seriesID],
						outputSeries.bucketLabelValue,
						posrange.PositionRange{},
					), ctx)
				}
				continue
			}

			groupID := outputSeries.outputID
			o.seriesBuckets[groupID] = append(o.seriesBuckets[groupID], promql.Bucket{
				UpperBound: outputSeries.upperBound,
				Count:      vector.Samples[i],
			})
		}

		buf[n].Reset(vector.T)
		for i, seriesID := range vector.HistogramIDs {
			groupID := o.outputIndex[seriesID].outputID
			// A classic histogram mapped to the same group means this step mixes
			// classic and native histogram representations. Suppress both.
			if len(o.seriesBuckets[groupID]) != 0 {
				warnings.AddToContext(annotations.NewMixedClassicNativeHistogramsWarning(
					o.baseSeriesNames[groupID],
					posrange.PositionRange{},
				), ctx)
				o.seriesBuckets[groupID] = o.seriesBuckets[groupID][:0]
				continue
			}

			switch o.operation {
			case histogramQuantileOperation:
				for quantileIndex := range len(o.scalarOps) {
					q, outputID := o.quantileAt(quantileIndex, stepIndex, groupID)
					value, annos := promql.HistogramQuantile(
						q,
						vector.Histograms[i],
						o.baseSeriesNames[groupID],
						posrange.PositionRange{},
					)
					buf[n].AppendSample(outputID, value)
					warnings.MergeToContext(annos, ctx)
				}
			case histogramFractionOperation:
				value, annos := promql.HistogramFraction(
					o.scalarPoints[0][stepIndex],
					o.scalarPoints[1][stepIndex],
					vector.Histograms[i],
					o.baseSeriesNames[groupID],
					posrange.PositionRange{},
				)
				buf[n].AppendSample(uint64(groupID), value)
				warnings.MergeToContext(annos, ctx)
			}
		}

		for groupID, stepBuckets := range o.seriesBuckets {
			if len(stepBuckets) == 0 {
				continue
			}

			switch o.operation {
			case histogramQuantileOperation:
				for quantileIndex := range len(o.scalarOps) {
					q, outputID := o.quantileAt(quantileIndex, stepIndex, groupID)
					// BucketQuantile expects at least two buckets.
					if len(stepBuckets) == 1 {
						buf[n].AppendSample(outputID, math.NaN())
						continue
					}
					value, forcedMonotonicity, _ := promql.BucketQuantile(q, stepBuckets)
					buf[n].AppendSample(outputID, value)
					if forcedMonotonicity {
						warnings.AddToContext(annotations.NewHistogramQuantileForcedMonotonicityInfo(
							o.baseSeriesNames[groupID],
							posrange.PositionRange{},
						), ctx)
					}
				}
			case histogramFractionOperation:
				value := promql.BucketFraction(
					o.scalarPoints[0][stepIndex],
					o.scalarPoints[1][stepIndex],
					stepBuckets,
				)
				buf[n].AppendSample(uint64(groupID), value)
			}
		}
		n++
	}

	return n, nil
}

func (o *histogramOperator) validateQuantileStep(stepIndex int, timestamp int64) error {
	if !o.addQuantileLabel {
		return nil
	}
	pointIndex := o.quantileCursor + stepIndex
	for quantileIndex := range o.quantilePoints {
		if pointIndex >= len(o.quantilePoints[quantileIndex]) {
			return errors.Newf("histogram_quantiles scalar argument %d ended before timestamp %d", quantileIndex, timestamp)
		}
		if pointTimestamp := o.quantilePoints[quantileIndex][pointIndex].timestamp; pointTimestamp != timestamp {
			return errors.Newf(
				"histogram_quantiles scalar argument %d has timestamp %d, expected %d",
				quantileIndex,
				pointTimestamp,
				timestamp,
			)
		}
	}
	return nil
}

func (o *histogramOperator) quantileAt(quantileIndex, stepIndex, groupID int) (float64, uint64) {
	if !o.addQuantileLabel {
		return o.scalarPoints[quantileIndex][stepIndex], uint64(groupID)
	}
	point := o.quantilePoints[quantileIndex][o.quantileCursor+stepIndex]
	return point.value, point.outputOffset + uint64(groupID)
}

func (o *histogramOperator) loadSeries(ctx context.Context) error {
	o.vectorBuf = make([]model.StepVector, o.stepsBatch)
	if !o.addQuantileLabel {
		o.scalarBufs = make([][]model.StepVector, len(o.scalarOps))
		o.scalarPoints = make([][]float64, len(o.scalarOps))
		for i := range o.scalarOps {
			o.scalarBufs[i] = make([]model.StepVector, o.stepsBatch)
			o.scalarPoints[i] = make([]float64, o.stepsBatch)
		}
	}

	series, err := o.vectorOp.Series(ctx)
	if err != nil {
		return err
	}

	var (
		hashBuf      = make([]byte, 0, 256)
		hasher       = xxhash.New()
		seriesHashes = make(map[uint64]int, len(series))
	)

	o.baseSeries = make([]labels.Labels, 0)
	o.baseSeriesNames = make([]string, 0)
	o.inputSeriesNames = make([]string, len(series))
	o.outputIndex = make([]*histogramSeries, len(series))
	builder := labels.ScratchBuilder{}
	for i, inputSeries := range series {
		hasBucketValue := true
		labelsWithoutBucket, bucketLabel := extlabels.DropBucketLabel(inputSeries, builder)
		upperBound, err := strconv.ParseFloat(bucketLabel.Value, 64)
		if err != nil {
			hasBucketValue = false
		}

		hasher.Reset()
		hashBuf = labelsWithoutBucket.Bytes(hashBuf)
		if _, err := hasher.Write(hashBuf); err != nil {
			return err
		}

		// Include reserved labels in the grouping hash so differently named input
		// histograms remain separate and duplicate output labels can be detected.
		outputLabels := extlabels.DropReserved(labelsWithoutBucket, builder)
		seriesHash := hasher.Sum64()
		groupID, ok := seriesHashes[seriesHash]
		if !ok {
			o.baseSeries = append(o.baseSeries, outputLabels)
			o.baseSeriesNames = append(o.baseSeriesNames, inputSeries.Get(labels.MetricName))
			groupID = len(o.baseSeries) - 1
			seriesHashes[seriesHash] = groupID
		}

		o.inputSeriesNames[i] = inputSeries.Get(labels.MetricName)
		o.outputIndex[i] = &histogramSeries{
			outputID:         groupID,
			upperBound:       upperBound,
			hasBucketValue:   hasBucketValue,
			bucketLabelValue: bucketLabel.Value,
		}
	}

	o.seriesBuckets = make([]promql.Buckets, len(o.baseSeries))
	o.badBucketWarned = make(map[uint64]bool)

	if o.addQuantileLabel {
		return o.preparePluralQuantiles(ctx)
	}
	o.series = o.baseSeries
	return nil
}

// preparePluralQuantiles evaluates scalar arguments before execution because
// their formatted values are part of the output series labels. Histogram input
// remains streaming; only one scalar value per quantile and step is retained.
func (o *histogramOperator) preparePluralQuantiles(ctx context.Context) error {
	o.quantilePoints = make([][]quantilePoint, len(o.scalarOps))
	o.series = make([]labels.Labels, 0, len(o.baseSeries)*len(o.scalarOps))

	for quantileIndex, scalarOp := range o.scalarOps {
		buf := make([]model.StepVector, o.stepsBatch)
		for {
			n, err := scalarOp.Next(ctx, buf)
			if err != nil {
				return err
			}
			if n == 0 {
				break
			}
			for stepIndex := range n {
				value := math.NaN()
				if len(buf[stepIndex].Samples) > 0 {
					value = buf[stepIndex].Samples[0]
				}
				o.warnInvalidQuantile(ctx, value)
				o.quantilePoints[quantileIndex] = append(o.quantilePoints[quantileIndex], quantilePoint{
					timestamp: buf[stepIndex].T,
					value:     value,
				})
			}
		}

		// Blocks are local to a quantile argument. Do not merge equal values
		// between arguments: simultaneous equal arguments must remain duplicate
		// label sets so the duplicate-label checker can reject them.
		blocks := make(map[string]uint64)
		labelBuilder := labels.NewBuilder(labels.EmptyLabels())
		for pointIndex := range o.quantilePoints[quantileIndex] {
			point := &o.quantilePoints[quantileIndex][pointIndex]
			quantileValue := labels.FormatOpenMetricsFloat(point.value)
			offset, ok := blocks[quantileValue]
			if !ok {
				offset = uint64(len(o.series))
				blocks[quantileValue] = offset
				for _, base := range o.baseSeries {
					labelBuilder.Reset(base)
					labelBuilder.Set(o.quantileLabel, quantileValue)
					o.series = append(o.series, labelBuilder.Labels())
				}
			}
			point.outputOffset = offset
		}
	}
	return nil
}

func (o *histogramOperator) resetBuckets() {
	for i := range o.seriesBuckets {
		o.seriesBuckets[i] = o.seriesBuckets[i][:0]
	}
}
