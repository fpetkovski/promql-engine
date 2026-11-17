// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package execution_test

import (
	"context"
	"testing"
	"time"

	"github.com/thanos-io/promql-engine/execution"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/internal/teststorage"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	engstorage "github.com/thanos-io/promql-engine/storage"
	promstorage "github.com/thanos-io/promql-engine/storage/prometheus"
	"github.com/thanos-io/promql-engine/warnings"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func TestBatchRequiresRoundRobin(t *testing.T) {
	store := teststorage.LoadedStorage(t, "load 30s\n m 1x24")
	defer store.Close()
	opts := batchTestOptions()
	ops, ctx, closeBatch := buildBatch(t, store, opts, []string{`sum(m)`, `sum(m)`}, true)
	defer closeBatch()
	for _, op := range ops {
		_, err := op.Series(ctx)
		testutil.Ok(t, err)
	}
	buf := make([]model.StepVector, opts.StepsBatch)
	n, err := ops[0].Next(ctx, buf)
	testutil.Ok(t, err)
	testutil.Equals(t, opts.StepsBatch, n)
	_, err = ops[0].Next(ctx, buf)
	require.EqualError(t, err, "shared batch reader advanced before its siblings")
}

func batchTestOptions() *query.Options {
	return &query.Options{
		Start: time.Unix(300, 0), End: time.Unix(720, 0), Step: 30 * time.Second,
		StepsBatch: 4, LookbackDelta: 5 * time.Minute, DecodingConcurrency: 1,
		NoStepSubqueryIntervalFn: func(time.Duration) time.Duration { return time.Minute },
		SampleTracker:            query.NewSampleTracker(0),
	}
}

func buildBatch(t testing.TB, store storage.Queryable, opts *query.Options, queries []string, share bool) ([]model.VectorOperator, context.Context, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(warnings.NewContext(t.Context()), 10*time.Second)
	roots := make([]logicalplan.Node, len(queries))
	scanners := make([]engstorage.Scanners, len(queries))
	closeBatch := func() {
		cancel()
		for _, scanner := range scanners {
			if scanner != nil {
				testutil.Ok(t, scanner.Close())
			}
		}
	}
	for i, text := range queries {
		ast, err := parser.NewParser(parser.Options{}).ParseExpr(text)
		testutil.Ok(t, err)
		plan, err := logicalplan.NewFromAST(ast, opts, logicalplan.PlanOptions{})
		testutil.Ok(t, err)
		plan, _ = plan.Optimize(logicalplan.DefaultOptimizers)
		roots[i] = plan.Root()
		scanners[i], err = promstorage.NewPrometheusScanners(store, opts, plan)
		testutil.Ok(t, err)
	}
	if share {
		roots = logicalplan.ShareCommonSubplans(roots)
	}
	batch, err := execution.NewBatch(ctx, roots, scanners, opts)
	testutil.Ok(t, err)
	return batch.Operators, ctx, func() {
		batch.Close()
		closeBatch()
	}
}
