// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/promql-engine/internal/teststorage"
	"github.com/thanos-io/promql-engine/query"
	engstorage "github.com/thanos-io/promql-engine/storage"

	"github.com/efficientgo/core/errors"
	"github.com/efficientgo/core/testutil"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func TestRangeBatch(t *testing.T) {
	store := teststorage.LoadedStorage(t, `
load 30s
    m{job="a",instance="0"} 0+1x24
    m{job="a",instance="1"} 0+2x24
    n{job="b"} 0+3x24
    requests_bucket{job="api",le="1"} 0+1x24
    requests_bucket{job="api",le="10"} 0+2x24
    requests_bucket{job="api",le="+Inf"} 0+3x24
    h{job="api"} {{schema:0 count:1 sum:2 buckets:[1]}}x24
    mixed{kind="float"} 1x24
    mixed{kind="histogram"} {{schema:0 count:1 sum:2 buckets:[1]}}x24
`)
	defer store.Close()
	for _, tc := range []struct {
		name    string
		queries []string
		shared  bool
	}{
		{"whole queries", []string{`sum(rate(m[5m]))`, `sum(rate(m[5m]))`}, true},
		{"quantiles", []string{`histogram_quantile(0.9, sum by(job, le)(rate(requests_bucket[5m])))`, `histogram_quantile(0.99, sum by(job, le)(rate(requests_bucket[5m])))`}, true},
		{"multiple groups", []string{`abs(sum(m))`, `-sum(m)`, `abs(sum(n))`, `-sum(n)`, `42`}, true},
		{"histograms", []string{`-sum(h)`, `sum(h)`}, true},
		{"shared warnings", []string{`-sum(mixed)`, `sum(mixed)`, `sum(n)`}, true},
		{"private warnings", []string{`histogram_quantile(-1, sum by(job, le)(rate(requests_bucket[5m])))`, `histogram_quantile(0.9, sum by(job, le)(rate(requests_bucket[5m])))`}, true},
		{"aggregate lookahead stays private", []string{`sum(rate(m[5m]))`, `max(rate(m[5m]))`}, false},
		{"initialization drain stays private", []string{`count_values("value", m)`, `abs(m)`}, false},
		{"whole initialization drain", []string{`count_values("value", m)`, `count_values("value", m)`}, true},
		{"subquery stays private", []string{`max_over_time(m[5m:1m])`, `abs(m)`}, false},
		{"whole subquery", []string{`max_over_time(m[5m:1m])`, `max_over_time(m[5m:1m])`}, true},
		{"whole step invariant", []string{`sum(m @ 300)`, `sum(m @ 300)`}, true},
		{"distinct offsets", []string{`sum(rate(m[5m] offset 1m))`, `sum(rate(m[5m] offset 2m))`}, false},
		{"empty and scalar", []string{`sum(missing)`, `sum(missing)`, `scalar(m)`, `time()`}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ng := engine.New(engine.Opts{Timeout: time.Minute, DecodingConcurrency: 2, SelectorBatchSize: 1})
			start, end, step := time.Unix(300, 0), time.Unix(720, 0), 30*time.Second
			batch, err := ng.NewRangeBatch(t.Context(), store, nil, tc.queries, start, end, step)
			require.NoError(t, err)
			defer batch.Close()
			var plan strings.Builder
			for _, root := range batch.Explain() {
				writeBatchPlan(&plan, root, "")
			}
			require.Equal(t, tc.shared, strings.Contains(plan.String(), "[shared id="), plan.String())
			results := batch.Exec(t.Context())
			for i, text := range tc.queries {
				q, err := ng.NewRangeQuery(t.Context(), store, nil, text, start, end, step)
				require.NoError(t, err)
				expected := q.Exec(t.Context())
				require.NoError(t, expected.Err)
				require.NoError(t, results[i].Err)
				testutil.WithGoCmp(cmpopts.EquateNaNs(), cmp.Comparer(labels.Equal)).Equals(t, expected.Value, results[i].Value)
				wantWarnings, wantInfos := expected.Warnings.AsStrings(text, 0, 0)
				gotWarnings, gotInfos := results[i].Warnings.AsStrings(text, 0, 0)
				require.ElementsMatch(t, wantWarnings, gotWarnings)
				require.ElementsMatch(t, wantInfos, gotInfos)
				q.Close()
			}
		})
	}
}

func TestRangeBatchPlanAndWork(t *testing.T) {
	store := teststorage.LoadedStorage(t, "load 30s\n m 1x24")
	defer store.Close()
	tracked := &batchQueryable{Queryable: store}
	tracker, err := promql.NewActiveQueryTracker(t.TempDir(), 1, promslog.NewNopLogger())
	require.NoError(t, err)
	defer tracker.Close()
	ng := engine.New(engine.Opts{Timeout: time.Second, DecodingConcurrency: 1, EnableAnalysis: true, EnablePerStepStats: true, ActiveQueryTracker: tracker})
	batch, err := ng.NewRangeBatch(t.Context(), tracked, nil, []string{`m`, `m`}, time.Unix(300, 0), time.Unix(720, 0), 30*time.Second)
	require.NoError(t, err)
	for _, root := range batch.Explain() {
		var plan strings.Builder
		writeBatchPlan(&plan, root, "")
		require.Equal(t, `[duplicateLabelCheck]
  [shared id=0 readers=2]
    [duplicateLabelCheck]
      [concurrent(buff=2)]
        [vectorSelector] {[__name__="m"]} 0 mod 1
`, plan.String())
	}
	for _, result := range batch.Exec(t.Context()) {
		require.NoError(t, result.Err)
	}
	require.Equal(t, int64(1), tracked.selected.Load(), "a common producer must scan only once")
	analysis := batch.Analyze()
	require.Equal(t, int64(15), analysis[0].TotalSamples()+analysis[1].TotalSamples(), "analysis must not double-count shared work")
	batch.Close()
	batch.Close()
	require.Equal(t, tracked.opened.Load(), tracked.closed.Load())
}

func TestRangeBatchHistogramOwnership(t *testing.T) {
	store := teststorage.LoadedStorage(t, "load 30s\n h {{schema:0 count:1 sum:2 buckets:[1]}}x24")
	defer store.Close()
	ng := engine.New(engine.Opts{Timeout: time.Minute, DecodingConcurrency: 1})
	batch, err := ng.NewRangeBatch(t.Context(), store, nil, []string{`sum(h)`, `sum(h)`}, time.Unix(300, 0), time.Unix(720, 0), 30*time.Second)
	require.NoError(t, err)
	defer batch.Close()
	results := batch.Exec(t.Context())
	first, err := results[0].Matrix()
	require.NoError(t, err)
	second, err := results[1].Matrix()
	require.NoError(t, err)
	first[0].Histograms[0].H.Mul(10)
	require.Equal(t, 2.0, second[0].Histograms[0].H.Sum)
	require.Equal(t, []float64{1}, second[0].Histograms[0].H.PositiveBuckets)
	require.Equal(t, 2.0, first[0].Histograms[1].H.Sum)
}

func TestRangeBatchErrors(t *testing.T) {
	store := teststorage.LoadedStorage(t, "load 30s\n a 1x24\n b 2x24")
	defer store.Close()
	for _, tc := range []struct {
		name    string
		queries []string
		failed  []bool
	}{
		{"first reader fails", []string{`-{__name__=~"a|b"}`, `{__name__=~"a|b"}`}, []bool{true, false}},
		{"last reader fails", []string{`{__name__=~"a|b"}`, `-{__name__=~"a|b"}`}, []bool{false, true}},
		{"producer fails", []string{`sum(failed)`, `-sum(failed)`, `a`}, []bool{true, true, false}},
		{"producer panics", []string{`sum(panics)`, `-sum(panics)`, `a`}, []bool{true, true, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracked := &batchQueryable{Queryable: store, beforeSelect: func(_ context.Context, matchers []*labels.Matcher) error {
				for _, m := range matchers {
					if m.Name == labels.MetricName && m.Value == "panics" {
						panic("storage panic")
					}
					if m.Name == labels.MetricName && m.Value == "failed" {
						return errors.New("storage failed")
					}
				}
				return nil
			}}
			ng := engine.New(engine.Opts{Timeout: time.Minute, DecodingConcurrency: 1})
			batch, err := ng.NewRangeBatch(t.Context(), tracked, nil, tc.queries, time.Unix(300, 0), time.Unix(720, 0), 30*time.Second)
			require.NoError(t, err)
			defer batch.Close()
			for i, result := range batch.Exec(t.Context()) {
				if tc.failed[i] {
					require.Error(t, result.Err)
					continue
				}
				matrix, err := result.Matrix()
				require.NoError(t, err)
				require.Len(t, matrix[0].Floats, 15, "survivors must finish after a reader detaches")
			}
		})
	}
}

func TestRangeBatchBudget(t *testing.T) {
	for _, tc := range []struct {
		name    string
		queries []string
		seconds int64
		limit   int
		fail    bool
	}{
		{"combined results exceed budget", []string{`vector(1)`, `vector(2)`}, 14, 20, true},
		{"combined results fit", []string{`vector(1)`, `vector(2)`}, 14, 1000, false},
		{"shared buffer and copy exceed budget", []string{`vector(1)`, `vector(1)`}, 0, 2, true},
		{"shared buffer charged once", []string{`vector(1)`, `vector(1)`}, 0, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ng := engine.New(engine.Opts{Timeout: time.Minute, MaxSamples: tc.limit})
			batch, err := ng.NewRangeBatch(t.Context(), &storage.MockQueryable{MockQuerier: storage.NoopQuerier()}, nil, tc.queries, time.Unix(0, 0), time.Unix(tc.seconds, 0), time.Second)
			require.NoError(t, err)
			defer batch.Close()
			for _, result := range batch.Exec(t.Context()) {
				if tc.fail {
					var limit query.ErrMaxSamplesExceeded
					require.ErrorAs(t, result.Err, &limit)
					require.Nil(t, result.Value)
				} else {
					require.NoError(t, result.Err)
				}
			}
		})
	}
}

func TestRangeBatchCancellation(t *testing.T) {
	store := teststorage.LoadedStorage(t, "load 30s\n m 1x24")
	defer store.Close()
	for _, mode := range []string{"cancel", "close", "exec context", "construction context", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			entered := make(chan struct{})
			var once sync.Once
			tracked := &batchQueryable{Queryable: store, beforeSelect: func(ctx context.Context, _ []*labels.Matcher) error {
				once.Do(func() { close(entered) })
				<-ctx.Done()
				return ctx.Err()
			}}
			timeout := time.Minute
			if mode == "timeout" {
				timeout = 20 * time.Millisecond
			}
			ng := engine.New(engine.Opts{Timeout: timeout, DecodingConcurrency: 1})
			buildCtx, cancelBuild := context.WithCancel(t.Context())
			defer cancelBuild()
			batch, err := ng.NewRangeBatch(buildCtx, tracked, nil, []string{`sum(m)`, `-sum(m)`}, time.Unix(300, 0), time.Unix(720, 0), 30*time.Second)
			require.NoError(t, err)
			defer batch.Close()
			execCtx, cancelExec := context.WithCancel(t.Context())
			defer cancelExec()
			finished := make(chan []*promql.Result, 1)
			go func() { finished <- batch.Exec(execCtx) }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("query did not reach storage")
			}
			switch mode {
			case "cancel":
				batch.Cancel()
			case "close":
				batch.Close()
			case "exec context":
				cancelExec()
			case "construction context":
				cancelBuild()
			}
			select {
			case results := <-finished:
				for _, result := range results {
					if mode == "timeout" {
						require.ErrorIs(t, result.Err, context.DeadlineExceeded)
					} else {
						require.ErrorIs(t, result.Err, context.Canceled)
					}
				}
			case <-time.After(5 * time.Second):
				t.Fatal("batch did not stop")
			}
			batch.Close()
			require.Equal(t, tracked.opened.Load(), tracked.closed.Load())
		})
	}
}

func TestRangeBatchConstruction(t *testing.T) {
	store := teststorage.LoadedStorage(t, "load 30s\n m 1x24")
	defer store.Close()
	for _, tc := range []struct {
		name    string
		queries []string
		failAt  int64
		fail    bool
	}{
		{"empty", nil, 0, true},
		{"parse error", []string{`m`, `sum(`}, 0, true},
		{"matrix", []string{`m[5m]`}, 0, true},
		{"unsupported", []string{`m`, `info(m)`}, 0, true},
		{"querier error", []string{`m`, `m`}, 2, true},
		{"close without executing", []string{`m`, `m`}, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracked := &batchQueryable{Queryable: store, failAt: tc.failAt}
			ng := engine.New(engine.Opts{Timeout: time.Minute, EnableExperimentalFunctions: true})
			batch, err := ng.NewRangeBatch(t.Context(), tracked, nil, tc.queries, time.Unix(300, 0), time.Unix(720, 0), 30*time.Second)
			if tc.fail {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				batch.Close()
				batch.Close()
				for _, result := range batch.Exec(t.Context()) {
					require.Error(t, result.Err)
				}
			}
			require.Equal(t, tracked.opened.Load(), tracked.closed.Load())
		})
	}
}

func TestRangeBatchCustomScanners(t *testing.T) {
	warn := errors.New("construction warning")
	scanner, err := newScannersWithWarns(warn, &query.Options{}, nil)
	require.NoError(t, err)
	tracked := &batchScanners{Scanners: scanner}
	ng := engine.NewWithScanners(engine.Opts{Timeout: time.Minute, DecodingConcurrency: 1}, tracked)
	batch, err := ng.MakeRangeBatch(t.Context(), nil, nil, []string{"m", "m", "vector(1)"}, time.Unix(0, 0), time.Unix(30, 0), time.Second)
	require.NoError(t, err)
	results := batch.Exec(t.Context())
	for i, result := range results {
		require.NoError(t, result.Err)
		if i < 2 {
			require.Contains(t, result.Warnings, warn.Error())
		} else {
			require.Empty(t, result.Warnings)
		}
	}
	batch.Close()
	batch.Close()
	require.Equal(t, 1, tracked.closes, "engine-supplied scanners belong to the batch, not each root")
}

func writeBatchPlan(out *strings.Builder, node *engine.ExplainOutputNode, indent string) {
	out.WriteString(indent + node.OperatorName + "\n")
	for _, child := range node.Children {
		writeBatchPlan(out, &child, indent+"  ")
	}
}

type batchScanners struct {
	engstorage.Scanners
	closes int
}

func (s *batchScanners) Close() error {
	s.closes++
	return s.Scanners.Close()
}

type batchQueryable struct {
	storage.Queryable
	opened, closed, selected atomic.Int64
	failAt                   int64
	beforeSelect             func(context.Context, []*labels.Matcher) error
}

func (q *batchQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	if q.failAt > 0 && q.opened.Load()+1 == q.failAt {
		return nil, errors.New("opening querier failed")
	}
	querier, err := q.Queryable.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	q.opened.Add(1)
	return &batchQuerier{Querier: querier, owner: q}, nil
}

type batchQuerier struct {
	storage.Querier
	owner *batchQueryable
}

func (q *batchQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	q.owner.selected.Add(1)
	if q.owner.beforeSelect != nil {
		if err := q.owner.beforeSelect(ctx, matchers); err != nil {
			return storage.ErrSeriesSet(err)
		}
	}
	return q.Querier.Select(ctx, sorted, hints, matchers...)
}

func (q *batchQuerier) Close() error {
	q.owner.closed.Add(1)
	return q.Querier.Close()
}
