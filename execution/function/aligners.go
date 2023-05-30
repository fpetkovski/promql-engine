package function

import (
	"fmt"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/histogram"

	"github.com/thanos-io/promql-engine/execution/parse"
	"github.com/thanos-io/promql-engine/parser"
)

type Aligner interface {
	AddSample(t int64, v float64, fh *histogram.FloatHistogram)
	GetResult(t int64) (v float64, fh *histogram.FloatHistogram, ok bool)
}

type newAlignerFunc func(step int64) Aligner

var aligners = map[string]newAlignerFunc{
	"rate": func(selectRange int64) Aligner {
		return &rateAligner{
			isCounter:   true,
			isRate:      true,
			selectRange: selectRange,
		}
	},
	"sum_over_time": func(selectRange int64) Aligner {
		return &sumAligner{}
	},
}

func NewAligner(name string, selectRange int64) (Aligner, error) {
	if aligner, ok := aligners[name]; ok {
		return aligner(selectRange), nil
	}

	msg := fmt.Sprintf("unknown function: %s", name)
	if _, ok := parser.Functions[name]; ok {
		return nil, errors.Wrap(parse.ErrNotImplemented, msg)
	}

	return nil, errors.Wrap(parse.ErrNotSupportedExpr, msg)
}

type rateAligner struct {
	isRate    bool
	isCounter bool

	selectRange int64

	numSamples     int64
	firstT         int64
	lastT          int64
	firstFloat     float64
	lastFloat      float64
	firstHistogram *histogram.FloatHistogram
	lastHistogram  *histogram.FloatHistogram
}

func (r *rateAligner) AddSample(t int64, v float64, fh *histogram.FloatHistogram) {
	if r.numSamples > 0 {
		r.lastFloat = v
		r.lastHistogram = fh
		r.lastT = t
		r.numSamples++
		if r.isCounter && r.lastFloat > v {
			v += r.lastFloat
		}
		return
	}

	r.firstFloat = v
	r.firstHistogram = fh

	r.lastFloat = v
	r.lastHistogram = fh

	r.firstT = t
	r.lastT = t
	r.numSamples++
}

func (r *rateAligner) GetResult(t int64) (v float64, fh *histogram.FloatHistogram, ok bool) {
	defer r.reset()

	if r.numSamples < 2 {
		return 0, nil, false
	}

	var (
		resultValue = r.lastFloat - r.firstFloat
		rangeStart  = t - r.selectRange
		rangeEnd    = t
	)

	// Duration between first/last samples and boundary of range.
	durationToStart := float64(r.firstT-rangeStart) / 1000
	durationToEnd := float64(rangeEnd-r.lastT) / 1000

	sampledInterval := float64(r.lastT-r.firstT) / 1000
	averageDurationBetweenSamples := sampledInterval / float64(r.numSamples-1)

	if r.isCounter && resultValue > 0 && r.firstFloat >= 0 {
		// Counters cannot be negative. If we have any slope at
		// all (i.e. resultValue went up), we can extrapolate
		// the zero point of the counter. If the duration to the
		// zero point is shorter than the durationToStart, we
		// take the zero point as the start of the series,
		// thereby avoiding extrapolation to negative counter
		// values.
		durationToZero := sampledInterval * (r.firstFloat / resultValue)
		if durationToZero < durationToStart {
			durationToStart = durationToZero
		}
	}

	// If the first/last samples are close to the boundaries of the range,
	// extrapolate the result. This is as we expect that another sample
	// will exist given the spacing between samples we've seen thus far,
	// with an allowance for noise.
	extrapolationThreshold := averageDurationBetweenSamples * 1.1
	extrapolateToInterval := sampledInterval

	if durationToStart < extrapolationThreshold {
		extrapolateToInterval += durationToStart
	} else {
		extrapolateToInterval += averageDurationBetweenSamples / 2
	}
	if durationToEnd < extrapolationThreshold {
		extrapolateToInterval += durationToEnd
	} else {
		extrapolateToInterval += averageDurationBetweenSamples / 2
	}
	factor := extrapolateToInterval / sampledInterval
	if r.isRate {
		factor /= float64(r.selectRange / 1000)
	}
	resultValue *= factor

	return resultValue, nil, true
}

func (r *rateAligner) reset() {
	r.numSamples = 0
	r.firstHistogram = nil
	r.lastHistogram = nil
}

type sumAligner struct {
	hasSamples bool
	sum        float64
}

func (s *sumAligner) AddSample(t int64, v float64, fh *histogram.FloatHistogram) {
	s.hasSamples = true
	s.sum += v
}

func (s *sumAligner) GetResult(t int64) (v float64, fh *histogram.FloatHistogram, ok bool) {
	result := s.sum
	s.hasSamples = false
	s.sum = 0
	return result, nil, true
}
