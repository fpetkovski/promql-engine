// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package exchange

import (
	"context"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/promql-engine/execution/model"
)

type dedupSample struct {
	t int64
	v float64
	h *histogram.FloatHistogram
}

// The dedupCache is an internal cache used to deduplicate samples inside a single step vector.
type dedupCache []dedupSample

// dedupOperator is a model.VectorOperator that deduplicates samples with
// same IDs inside a single model.StepVector.
// Deduplication is done using a last-sample-wins strategy, which means that
// if multiple samples with the same ID are present in a StepVector, dedupOperator
// will keep the last sample in that vector.
type dedupOperator struct {
	pool *model.VectorPool
	next model.VectorOperator
	// outputIndex is a slice that is used as an index from input sample ID to output sample ID.
	outputIndex []uint64
	dedupCache  dedupCache
	model.OperatorTelemetry
}

func NewDedupOperator(pool *model.VectorPool, next model.VectorOperator) model.VectorOperator {
	d := &dedupOperator{
		next: next,
		pool: pool,
	}
	d.OperatorTelemetry = &model.TrackedTelemetry{}
	return d
}

func (d *dedupOperator) Analyze() (model.OperatorTelemetry, []model.ObservableVectorOperator) {
	d.SetName("[*dedup]")
	next := make([]model.ObservableVectorOperator, 0, 1)
	if obsnext, ok := d.next.(model.ObservableVectorOperator); ok {
		next = append(next, obsnext)
	}
	return d, next
}

func (d *dedupOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	start := time.Now()
	in, err := d.next.Next(ctx)
	if err != nil {
		return nil, err
	}
	if in == nil {
		return nil, nil
	}

	result := d.pool.GetVectorBatch()
	for _, vector := range in {
		for i, inputSampleID := range vector.SampleIDs {
			outputSampleID := d.outputIndex[inputSampleID]
			d.dedupCache[outputSampleID].t = vector.T
			d.dedupCache[outputSampleID].v = vector.Samples[i]
		}

		for i, inputSampleID := range vector.HistogramIDs {
			outputSampleID := d.outputIndex[inputSampleID]
			d.dedupCache[outputSampleID].t = vector.T
			d.dedupCache[outputSampleID].h = vector.Histograms[i]
		}

		out := d.pool.GetStepVector(vector.T)
		for outputSampleID, sample := range d.dedupCache {
			// To avoid clearing the dedup cache for each step vector, we use the `t` field
			// to detect whether a sample for the current step should be mapped to the output.
			// If the timestamp of the sample does not match the input vector timestamp, it means that
			// the sample was added in a previous iteration and should be skipped.
			if sample.t == vector.T {
				if sample.h == nil {
					out.AppendSample(d.pool, uint64(outputSampleID), sample.v)
				} else {
					out.AppendHistogram(d.pool, uint64(outputSampleID), sample.h)
				}
			}
		}
		result = append(result, out)
	}
	d.AddExecutionTimeTaken(time.Since(start))

	return result, nil
}

func (d *dedupOperator) Series(ctx context.Context) model.LabelsIterator {
	series := d.next.Series(ctx)
	outputIndex := make(map[uint64]uint64)
	inputIndex := make([]uint64, series.Size())
	hashBuf := make([]byte, 0, 128)
	outputSeries := make([]labels.Labels, 0)
	inputSeriesID := -1
	for series.Next() {
		inputSeriesID++
		inputSeries := series.At()
		hash := hashSeries(hashBuf, inputSeries)

		inputIndex[inputSeriesID] = hash
		outputSeriesID, ok := outputIndex[hash]
		if !ok {
			outputSeriesID = uint64(len(outputSeries))
			outputSeries = append(outputSeries, inputSeries)
		}
		outputIndex[hash] = outputSeriesID
	}

	d.outputIndex = make([]uint64, len(inputIndex))
	for inputSeriesID, hash := range inputIndex {
		outputSeriesID := outputIndex[hash]
		d.outputIndex[inputSeriesID] = outputSeriesID
	}
	d.dedupCache = make(dedupCache, len(outputIndex))
	for i := range d.dedupCache {
		d.dedupCache[i].t = -1
	}

	return model.NewLabelSliceIterator(outputSeries)
}

func (d *dedupOperator) GetPool() *model.VectorPool {
	return d.pool
}

func (d *dedupOperator) Explain() (me string, next []model.VectorOperator) {
	return "[*dedup]", []model.VectorOperator{d.next}
}

func hashSeries(hashBuf []byte, inputSeries labels.Labels) uint64 {
	hashBuf = hashBuf[:0]
	hash := xxhash.Sum64(inputSeries.Bytes(hashBuf))
	return hash
}
