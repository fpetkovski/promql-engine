// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package prometheus

import (
	"context"
	"sort"
	"time"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	"github.com/thanos-io/promql-engine/ringbuffer"
	"github.com/thanos-io/promql-engine/warnings"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
)

// extendedSelector evaluates the widened window read by matrixSelector. Both
// range modifiers use Prometheus' range functions, including histogram math,
// counter resets, and warnings. Scratch space is shared across a shard's series.
type extendedSelector struct {
	before, after int64
	call          promql.FunctionCall
	args          parser.Expressions
	pos           posrange.PositionRange
	input         [1]promql.Series
	helper        promql.EvalNodeHelper
}

func newExtendedSelector(
	selector SeriesSelector,
	opts *query.Options,
	vs *logicalplan.VectorSelector,
	selectRange time.Duration,
	functionName string,
	shard, numShards int,
) model.VectorOperator {
	before, after := vs.Mode().Window(selectRange.Milliseconds(), opts.LookbackDelta.Milliseconds())
	ext := &extendedSelector{before: before, after: after, pos: vs.PositionRange()}
	if functionName != "" {
		ext.call = promql.FunctionCalls[functionName]
		ext.helper.Out = make(promql.Vector, 0, 1)
		ext.args = parser.Expressions{&parser.MatrixSelector{
			VectorSelector: vs.VectorSelector,
			Range:          selectRange,
			EndPos:         ext.pos.End,
		}}
	}
	o := newMatrixSelector(selector, functionName, nil, 0, 0, opts, selectRange, vs.Offset, vs.BatchSize, shard, numShards)
	o.extended = ext
	return o.instrument()
}

func (e *extendedSelector) eval(ctx context.Context, f ringbuffer.FunctionArgs, metric labels.Labels) (float64, *histogram.FloatHistogram, bool, warnings.Warnings, error) {
	if len(f.Samples) == 0 {
		return 0, nil, false, 0, nil
	}
	if e.call == nil {
		v, ok := smoothPoint(ctx, f.Samples, f.StepTime-f.Offset, metric.Get(labels.MetricName), e.pos)
		return v.F, v.H, ok, 0, nil
	}

	s := &e.input[0]
	s.Metric = metric
	for _, p := range f.Samples {
		if p.V.H != nil {
			s.Histograms = append(s.Histograms, promql.HPoint{T: p.T, H: p.V.H})
		} else {
			s.Floats = append(s.Floats, promql.FPoint{T: p.T, F: p.V.F})
		}
	}
	e.helper.Ts = f.StepTime
	out, annos := e.call(nil, e.input[:], e.args, &e.helper)
	if len(annos) > 0 {
		warnings.MergeToContext(annos, ctx)
	}
	clear(s.Histograms) // Histograms are owned by the window, not by the scratch space.
	s.Histograms = s.Histograms[:0]
	s.Floats = s.Floats[:0]
	if len(out) == 0 {
		return 0, nil, false, 0, nil
	}
	v := out[0]
	out[0] = promql.Sample{}
	return v.F, v.H, true, 0, nil
}

func smoothPoint(ctx context.Context, samples []ringbuffer.Sample, t int64, metric string, pos posrange.PositionRange) (ringbuffer.Value, bool) {
	isHistogram := samples[0].V.H != nil
	for _, s := range samples[1:] {
		if (s.V.H != nil) != isHistogram {
			warnings.AddToContext(annotations.NewMixedFloatsHistogramsWarning(metric, pos), ctx)
			return ringbuffer.Value{}, false
		}
	}

	next := sort.Search(len(samples), func(i int) bool { return samples[i].T >= t })
	switch {
	case next < len(samples) && samples[next].T == t:
		v := samples[next].V
		if isHistogram {
			v.H = v.H.Copy()
		}
		return v, true
	case next == 0:
		return ringbuffer.Value{}, false
	case next == len(samples):
		v := samples[next-1].V
		if isHistogram {
			v.H = v.H.Copy()
			v.H.CounterResetHint = histogram.UnknownCounterReset
		}
		return v, true
	default:
		left, right := samples[next-1], samples[next]
		if isHistogram {
			h := smoothHistogram(ctx, left.V.H, right.V.H, float64(t-left.T)/float64(right.T-left.T), metric, pos)
			return ringbuffer.Value{H: h}, h != nil
		}
		v := left.V.F + (right.V.F-left.V.F)*float64(t-left.T)/float64(right.T-left.T)
		return ringbuffer.Value{F: v}, true
	}
}

func smoothHistogram(ctx context.Context, left, right *histogram.FloatHistogram, fraction float64, metric string, pos posrange.PositionRange) *histogram.FloatHistogram {
	if left.UsesCustomBuckets() != right.UsesCustomBuckets() {
		warnings.AddToContext(annotations.NewMixedExponentialCustomHistogramsWarning(metric, pos), ctx)
		return nil
	}
	h := right.Copy()
	if (left.CounterResetHint != histogram.GaugeType || right.CounterResetHint != histogram.GaugeType) && right.DetectReset(left) {
		return h.Mul(fraction)
	}
	_, _, reconciled, err := h.Sub(left)
	if err != nil {
		return nil
	}
	if reconciled {
		warnings.AddToContext(annotations.NewMismatchedCustomBucketsHistogramsInfo(pos, annotations.HistogramSub), ctx)
	}
	h.Mul(fraction)
	_, _, reconciled, err = h.Add(left)
	if err != nil {
		return nil
	}
	if reconciled {
		warnings.AddToContext(annotations.NewMismatchedCustomBucketsHistogramsInfo(pos, annotations.HistogramAdd), ctx)
	}
	return h
}
