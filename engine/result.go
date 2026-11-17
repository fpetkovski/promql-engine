// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine

import (
	"sort"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
)

// resultCollector is shared by single-query execution and the batch driver.
// Histogram ownership transfers from the operator buffer to the result.
type resultCollector struct {
	series  []promql.Series
	opts    *query.Options
	samples int
}

func newResultCollector(series []labels.Labels, opts *query.Options) *resultCollector {
	c := &resultCollector{series: make([]promql.Series, len(series)), opts: opts}
	for i, s := range series {
		c.series[i].Metric = s
	}
	return c
}

func (c *resultCollector) append(buf []model.StepVector) error {
	// Scalars can produce samples even when Series returns nil.
	if len(c.series) == 0 && len(buf) > 0 {
		c.series = make([]promql.Series, len(buf[0].Samples))
	}
	var samples int
	for _, vector := range buf {
		samples += len(vector.SampleIDs)
		for j, id := range vector.SampleIDs {
			s := &c.series[id]
			if s.Floats == nil {
				s.Floats = make([]promql.FPoint, 0, c.opts.TotalSteps())
			}
			s.Floats = append(s.Floats, promql.FPoint{T: vector.T, F: vector.Samples[j]})
		}
		for j, id := range vector.HistogramIDs {
			s := &c.series[id]
			if s.Histograms == nil {
				s.Histograms = make([]promql.HPoint, 0, c.opts.TotalSteps())
			}
			s.Histograms = append(s.Histograms, promql.HPoint{T: vector.T, H: vector.Histograms[j]})
			samples += telemetry.CalculateHistogramSampleCount(vector.Histograms[j])
		}
	}
	c.samples += samples
	c.opts.SampleTracker.Add(samples)
	return c.opts.SampleTracker.CheckLimit()
}

func (c *resultCollector) matrix() (promql.Matrix, error) {
	matrix := make(promql.Matrix, 0, len(c.series))
	for _, s := range c.series {
		if len(s.Floats)+len(s.Histograms) != 0 {
			matrix = append(matrix, s)
		}
	}
	sort.Sort(matrix)
	if matrix.ContainsSameLabelset() {
		return nil, extlabels.ErrDuplicateLabelSet
	}
	return matrix, nil
}

func (c *resultCollector) discard() {
	c.opts.SampleTracker.Remove(c.samples)
	c.samples = 0
	c.series = nil
}
