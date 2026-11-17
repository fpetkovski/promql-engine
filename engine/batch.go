// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/thanos-io/promql-engine/execution"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	engstorage "github.com/thanos-io/promql-engine/storage"
	"github.com/thanos-io/promql-engine/warnings"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

// RangeBatch evaluates range queries on a common grid and shares exact common
// subplans where lockstep execution is safe. MaxSamples and Timeout apply to the
// entire batch; the active query tracker admits it as one execution. Results
// preserve input order. Construction is atomic; execution errors affect only the
// dependent queries, except cancellation and sample-limit errors abort all queries.
//
// Exec may be called once. Always Close the batch, even if it is never executed.
// Cancel and Close are safe to call concurrently with Exec. Explain and Analyze
// wait for an in-flight Exec; shared work appears once in batch analysis.
type RangeBatch struct {
	mu       sync.Mutex
	engine   *Engine
	opts     *query.Options
	plans    []logicalplan.Plan
	warns    []annotations.Annotations
	exec     *execution.Batch
	scanners []engstorage.Scanners
	text     string
	ctx      context.Context
	cancel   context.CancelFunc
	executed bool
	closed   bool
}

// NewRangeBatch uses the same options as NewRangeQuery. Use MakeRangeBatch for
// engine-specific options such as selector batch size and logical optimizers.
func (e *Engine) NewRangeBatch(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, queries []string, start, end time.Time, step time.Duration) (*RangeBatch, error) {
	qOpts, err := fromPromQLOpts(opts)
	if err != nil {
		return nil, err
	}
	return e.MakeRangeBatch(ctx, q, qOpts, queries, start, end, step)
}

// MakeRangeBatch accepts engine-specific options, as MakeRangeQuery does.
func (e *Engine) MakeRangeBatch(ctx context.Context, q storage.Queryable, opts *QueryOpts, queries []string, start, end time.Time, step time.Duration) (_ *RangeBatch, err error) {
	if len(queries) == 0 {
		return nil, errors.New("range batch requires at least one query")
	}
	if step.Milliseconds() <= 0 || end.Before(start) {
		return nil, errors.New("range batch requires a positive millisecond step and end >= start")
	}
	if q == nil && e.scanners == nil {
		return nil, errors.New("range batch requires storage")
	}
	if opts == nil {
		opts = &QueryOpts{}
	}
	ctx, cancel := context.WithCancel(ctx)
	b := &RangeBatch{
		engine: e, opts: e.makeQueryOpts(start, end, step, opts),
		plans: make([]logicalplan.Plan, len(queries)), warns: make([]annotations.Annotations, len(queries)),
		text: strings.Join(queries, "; "), ctx: ctx, cancel: cancel,
	}
	defer func() {
		if err != nil {
			b.Close()
		}
	}()
	idx, err := e.activeQueryTracker.Insert(ctx, b.text)
	if err != nil {
		return nil, err
	}
	defer e.activeQueryTracker.Delete(idx)

	roots := make([]logicalplan.Node, len(queries))
	planOpts := logicalplan.PlanOptions{DisableDuplicateLabelCheck: e.disableDuplicateLabelChecks}
	for i, text := range queries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		expr, err := e.parser.ParseExpr(text)
		if err != nil {
			return nil, errors.Wrapf(err, "query %d", i)
		}
		if expr.Type() != parser.ValueTypeVector && expr.Type() != parser.ValueTypeScalar {
			return nil, errors.Newf("query %d: invalid expression type %q for range query, must be Scalar or instant Vector", i, parser.DocumentedType(expr.Type()))
		}
		plan, err := logicalplan.NewFromAST(expr, b.opts, planOpts)
		if err != nil {
			return nil, errors.Wrapf(err, "query %d: creating plan", i)
		}
		b.plans[i], b.warns[i] = plan.Optimize(e.getLogicalOptimizers(opts))
		roots[i] = b.plans[i].Root()
	}
	// Open scanners against the original query ranges, before installing shared
	// boundaries. A shared producer uses the scanner of its first occurrence.
	scanners := make([]engstorage.Scanners, len(queries))
	for i, plan := range b.plans {
		scanner, err := e.storageScanners(q, b.opts, plan)
		if err != nil {
			return nil, errors.Wrapf(err, "query %d: creating storage scanners", i)
		}
		scanners[i] = scanner
		if e.scanners == nil || i == 0 {
			b.scanners = append(b.scanners, scanner)
		}
	}
	b.exec, err = execution.NewBatch(ctx, logicalplan.ShareCommonSubplans(roots), scanners, b.opts)
	if err != nil {
		return nil, err
	}
	for i := range b.warns {
		b.warns[i].Merge(b.exec.Warnings[i])
	}
	e.metrics.totalQueries.Add(float64(len(queries)))
	return b, nil
}

// Exec evaluates the batch once and returns one result per input expression.
func (b *RangeBatch) Exec(ctx context.Context) []*promql.Result {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.executed || b.closed {
		return b.errorResults(errors.New("range batch is closed or already executed"))
	}
	b.executed = true
	defer b.cancel()
	defer b.exec.Close()

	ctx, cancel := context.WithTimeout(ctx, b.engine.timeout)
	defer cancel()
	stop := context.AfterFunc(b.ctx, cancel)
	defer stop()
	if err := b.ctx.Err(); err != nil {
		return b.errorResults(err)
	}
	idx, err := b.engine.activeQueryTracker.Insert(ctx, b.text)
	if err != nil {
		return b.errorResults(err)
	}
	defer b.engine.activeQueryTracker.Delete(idx)
	b.engine.metrics.currentQueries.Add(float64(len(b.plans)))
	defer b.engine.metrics.currentQueries.Sub(float64(len(b.plans)))

	states := make([]batchQueryState, len(b.plans))
	results := make([]*promql.Result, len(states))
	for i := range states {
		rootCtx := warnings.NewContext(ctx)
		warnings.MergeToContext(b.warns[i], rootCtx)
		states[i] = batchQueryState{
			ctx: rootCtx, plan: b.plans[i], op: b.exec.Operators[i],
			buf: make([]model.StepVector, b.opts.StepsBatch),
		}
	}
	for active := len(states); active > 0; {
		for i := range states {
			if err := ctx.Err(); err != nil {
				return b.errorResults(err)
			}
			if results[i] != nil {
				continue
			}
			s := &states[i]
			done, err := s.next(b.engine, b.opts)
			var sampleLimit query.ErrMaxSamplesExceeded
			if errors.As(err, &sampleLimit) {
				return b.errorResults(err)
			}
			if err == nil {
				if limitErr := b.opts.SampleTracker.CheckLimit(); limitErr != nil {
					return b.errorResults(limitErr)
				}
			}
			if !done && err == nil {
				continue
			}
			b.exec.Detach(i)
			active--
			results[i] = &promql.Result{Err: err, Warnings: warnings.FromContext(s.ctx)}
			if err == nil {
				results[i].Value = s.value
			} else if s.collector != nil {
				s.collector.discard()
			}
			s.collector = nil
			s.buf = nil
		}
	}
	if err := ctx.Err(); err != nil {
		return b.errorResults(err)
	}
	if err := b.opts.SampleTracker.CheckLimit(); err != nil {
		return b.errorResults(err)
	}
	return results
}

func (b *RangeBatch) Cancel() { b.cancel() }

func (b *RangeBatch) Close() {
	b.cancel()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	if b.exec != nil {
		b.exec.Close()
	}
	for _, scanner := range b.scanners {
		if err := scanner.Close(); err != nil {
			b.engine.logger.Warn("error closing batch storage scanners", "err", err)
		}
	}
	b.scanners = nil
}

func (b *RangeBatch) Explain() []*ExplainOutputNode {
	b.mu.Lock()
	defer b.mu.Unlock()
	plans := make([]*ExplainOutputNode, len(b.exec.Operators))
	for i, op := range b.exec.Operators {
		plans[i] = explainVector(op)
	}
	return plans
}

// Analyze attributes a shared producer to its first occurrence only. The
// returned trees describe physical batch work, not independent query costs.
func (b *RangeBatch) Analyze() []*AnalyzeOutputNode {
	b.mu.Lock()
	defer b.mu.Unlock()
	seen := make(map[model.VectorOperator]struct{})
	plans := make([]*AnalyzeOutputNode, len(b.exec.Operators))
	for i, op := range b.exec.Operators {
		plans[i] = analyzeQueryWithSeen(op, seen)
	}
	return plans
}

func (b *RangeBatch) errorResults(err error) []*promql.Result {
	results := make([]*promql.Result, len(b.plans))
	for i := range results {
		results[i] = &promql.Result{Err: err}
	}
	return results
}

type batchQueryState struct {
	ctx       context.Context
	plan      logicalplan.Plan
	op        model.VectorOperator
	collector *resultCollector
	buf       []model.StepVector
	value     promql.Matrix
}

func (s *batchQueryState) next(engine *Engine, opts *query.Options) (done bool, err error) {
	defer recoverEngine(engine.logger, s.plan, &err)
	if s.collector == nil {
		series, err := s.op.Series(s.ctx)
		if err != nil {
			return true, err
		}
		s.collector = newResultCollector(series, opts)
	}
	n, err := s.op.Next(s.ctx, s.buf)
	if err != nil {
		return true, err
	}
	if n == 0 {
		s.value, err = s.collector.matrix()
		return true, err
	}
	return false, s.collector.append(s.buf[:n])
}
