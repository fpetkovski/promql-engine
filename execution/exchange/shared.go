// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package exchange

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/query"
	"github.com/thanos-io/promql-engine/warnings"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/annotations"
)

// Shared retains one producer batch. Readers must be driven in lockstep;
// advancing twice before one's siblings is an error, never a blocking wait.
// All reader contexts must have the batch's lifetime, not a per-query lifetime.
type Shared struct {
	mu        sync.Mutex
	input     model.VectorOperator
	opts      *query.Options
	id        int
	consumers int
	active    int
	remaining int
	sequence  int
	buffer    []model.StepVector
	samples   int
	n         int
	err       error
	done      bool

	once   sync.Once
	ctx    context.Context
	cancel context.CancelFunc
	warns  annotations.Annotations
	series []labels.Labels
}

func NewShared(ctx context.Context, id int, input model.VectorOperator, consumers int, opts *query.Options) *Shared {
	return &Shared{
		input: input, opts: opts, id: id, consumers: consumers, active: consumers,
		sequence: -1, buffer: make([]model.StepVector, opts.StepsBatch),
		warns: warnings.FromContext(ctx),
	}
}

func (s *Shared) Reader() *sharedReader {
	r := &sharedReader{shared: s}
	r.OperatorTelemetry = telemetry.NewTelemetry(r, s.opts)
	return r
}

type sharedReader struct {
	telemetry.OperatorTelemetry
	shared   *Shared
	sequence int
	closed   bool
}

func (r *sharedReader) String() string {
	return fmt.Sprintf("[shared id=%d readers=%d]", r.shared.id, r.shared.consumers)
}

func (r *sharedReader) Explain() []model.VectorOperator {
	return []model.VectorOperator{r.shared.input}
}

func (r *sharedReader) Series(ctx context.Context) ([]labels.Labels, error) {
	s := r.shared
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.init(ctx)
	warnings.MergeToContext(warnings.FromContext(s.ctx), ctx)
	// Consumers may replace entries, but labels themselves are immutable.
	return slices.Clone(s.series), s.err
}

func (r *sharedReader) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	s := r.shared
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if r.closed {
		return 0, nil
	}
	s.init(ctx)
	if !s.done && r.sequence > s.sequence {
		if s.remaining != 0 {
			return 0, errors.New("shared batch reader advanced before its siblings")
		}
		s.releaseBuffer()
		s.pull()
		for _, v := range s.buffer[:s.n] {
			s.samples += len(v.Samples)
			for _, h := range v.Histograms {
				s.samples += telemetry.CalculateHistogramSampleCount(h)
			}
		}
		s.opts.SampleTracker.Add(s.samples)
		if s.err == nil {
			s.err = s.opts.SampleTracker.CheckLimit()
		}
		s.done = s.err != nil || s.n == 0
		s.sequence++
		s.remaining = s.active
		if s.done {
			s.releaseBuffer()
		}
	}
	warnings.MergeToContext(warnings.FromContext(s.ctx), ctx)
	if s.done {
		return 0, s.err
	}
	if len(buf) < s.n {
		return 0, errors.New("shared batch consumer buffer is too small")
	}
	// Reserve the copy before allocating it. On return it is an ordinary
	// operator buffer; the result collector accounts for retained output.
	s.opts.SampleTracker.Add(s.samples)
	defer s.opts.SampleTracker.Remove(s.samples)
	if err := s.opts.SampleTracker.CheckLimit(); err != nil {
		return 0, err
	}
	for i, src := range s.buffer[:s.n] {
		dst := &buf[i]
		clear(dst.Histograms[:cap(dst.Histograms)])
		dst.Reset(src.T)
		dst.AppendSamples(src.SampleIDs, src.Samples)
		for j, h := range src.Histograms {
			dst.AppendHistogram(src.HistogramIDs[j], h.Copy())
		}
	}
	r.sequence++
	s.remaining--
	return s.n, nil
}

// Close detaches a reader on EOF or failure, including before its first read.
func (r *sharedReader) Close() {
	s := r.shared
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	s.active--
	if r.sequence <= s.sequence && !s.done {
		s.remaining--
	}
	if s.active == 0 {
		if s.cancel != nil {
			s.cancel()
		}
		s.releaseBuffer()
		s.buffer = nil
	}
}

func (s *Shared) init(ctx context.Context) {
	s.once.Do(func() {
		defer s.recover()
		s.ctx, s.cancel = context.WithCancel(warnings.NewContext(ctx))
		warnings.MergeToContext(s.warns, s.ctx)
		s.series, s.err = s.input.Series(s.ctx)
		s.done = s.err != nil
	})
}

func (s *Shared) pull() {
	defer s.recover()
	s.n, s.err = s.input.Next(s.ctx, s.buffer)
}

// A producer panic must be cached like an ordinary producer error. In
// particular, sync.Once must not leave later readers observing a successful init.
func (s *Shared) recover() {
	if value := recover(); value != nil {
		s.n = 0
		s.err = errors.Newf("unexpected panic: %v", value)
		s.done = true
	}
}

func (s *Shared) releaseBuffer() {
	s.opts.SampleTracker.Remove(s.samples)
	s.samples = 0
	for i := range s.buffer {
		clear(s.buffer[i].Histograms[:cap(s.buffer[i].Histograms)])
		s.buffer[i].Reset(0)
	}
}
