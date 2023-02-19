// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package model

import (
	"arena"
	"sync"

	"github.com/prometheus/prometheus/model/histogram"
)

type PoolFactory struct {
	pools []*VectorPool
}

func NewPoolFactory() *PoolFactory {
	return &PoolFactory{}
}

func (p *PoolFactory) NewPool(stepsBatch int) *VectorPool {
	pool := NewVectorPool(stepsBatch)
	p.pools = append(p.pools, pool)
	return pool
}

func (p *PoolFactory) Close() {
	for _, pool := range p.pools {
		pool.memArena.Free()
	}
}

type VectorPool struct {
	vectors sync.Pool

	stepSize   int
	samples    sync.Pool
	sampleIDs  sync.Pool
	histograms sync.Pool

	memArena *arena.Arena
}

func NewVectorPool(stepsBatch int) *VectorPool {
	memBuffer := arena.NewArena()

	pool := &VectorPool{
		memArena: memBuffer,
	}
	pool.vectors = sync.Pool{
		New: func() any {
			sv := arena.MakeSlice[StepVector](memBuffer, 0, stepsBatch)
			return &sv
		},
	}
	pool.samples = sync.Pool{
		New: func() any {
			samples := arena.MakeSlice[float64](memBuffer, 0, pool.stepSize)
			return &samples
		},
	}
	pool.sampleIDs = sync.Pool{
		New: func() any {
			sampleIDs := arena.MakeSlice[uint64](memBuffer, 0, pool.stepSize)
			return &sampleIDs
		},
	}
	pool.histograms = sync.Pool{
		New: func() any {
			histograms := make([]*histogram.FloatHistogram, 0, pool.stepSize)
			return &histograms
		},
	}

	return pool
}

func (p *VectorPool) Close() error {
	p.memArena.Free()
	return nil
}

func (p *VectorPool) GetVectorBatch() []StepVector {
	return *p.vectors.Get().(*[]StepVector)
}

func (p *VectorPool) PutVectors(vector []StepVector) {
	vector = vector[:0]
	p.vectors.Put(&vector)
}

func (p *VectorPool) GetStepVector(t int64) StepVector {
	return StepVector{T: t}
}

func (p *VectorPool) getSampleBuffers() ([]uint64, []float64) {
	return *p.sampleIDs.Get().(*[]uint64), *p.samples.Get().(*[]float64)
}

func (p *VectorPool) getHistogramBuffers() ([]uint64, []*histogram.FloatHistogram) {
	return *p.sampleIDs.Get().(*[]uint64), *p.histograms.Get().(*[]*histogram.FloatHistogram)
}

func (p *VectorPool) PutStepVector(v StepVector) {
	if v.SampleIDs != nil {
		v.SampleIDs = v.SampleIDs[:0]
		p.sampleIDs.Put(&v.SampleIDs)

		v.Samples = v.Samples[:0]
		p.samples.Put(&v.Samples)
	}

	if v.HistogramIDs != nil {
		v.Histograms = v.Histograms[:0]
		p.histograms.Put(&v.Histograms)

		v.HistogramIDs = v.HistogramIDs[:0]
		p.sampleIDs.Put(&v.HistogramIDs)
	}
}

func (p *VectorPool) SetStepSize(n int) {
	p.stepSize = n
}
