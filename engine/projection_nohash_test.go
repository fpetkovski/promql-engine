// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/promql-engine/logicalplan"

	"github.com/efficientgo/core/testutil"
	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
)

// noHashQueryable applies the projection hints like projectionQueryable does,
// but does not add a series hash label to keep trimmed series distinct.
type noHashQueryable struct{ storage.Queryable }

func (q *noHashQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	querier, err := q.Queryable.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &noHashQuerier{Querier: querier}, nil
}

type noHashQuerier struct{ storage.Querier }

func (m *noHashQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return noHashSeriesSet{SeriesSet: m.Querier.Select(ctx, sortSeries, hints, matchers...), hints: hints}
}

func (m *noHashQuerier) LabelValues(ctx context.Context, name string, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (m *noHashQuerier) LabelNames(ctx context.Context, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (m *noHashQuerier) Close() error { return nil }

type noHashSeriesSet struct {
	storage.SeriesSet
	hints *storage.SelectHints
}

func (m noHashSeriesSet) At() storage.Series {
	series := m.SeriesSet.At()
	if series == nil || m.hints == nil {
		return series
	}
	if !m.hints.ProjectionInclude && len(m.hints.ProjectionLabels) == 0 {
		return series
	}

	b := labels.NewBuilder(labels.EmptyLabels())
	series.Labels().Range(func(l labels.Label) {
		if slices.Contains(m.hints.ProjectionLabels, l.Name) == m.hints.ProjectionInclude {
			b.Set(l.Name, l.Value)
		}
	})
	return &noHashSeries{Series: series, lset: b.Labels()}
}

type noHashSeries struct {
	storage.Series
	lset labels.Labels
}

func (s *noHashSeries) Labels() labels.Labels { return s.lset }

func (s *noHashSeries) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	return s.Series.Iterator(it)
}

// TestProjectionWithoutSeriesHashLabel covers projections against a storage
// that does not keep trimmed series distinct with a hash label. Distinct series
// then reach the engine with identical label sets, so the duplicate labelset
// checks inside the projected subtree must not fire on them.
func TestProjectionWithoutSeriesHashLabel(t *testing.T) {
	t.Parallel()

	// Two series share the projected label (pod) and differ only in a label the
	// projection drops (instance).
	load := `load 30s
		http_requests_total{pod="nginx-1", job="app", instance="1"} 1+1x40
		http_requests_total{pod="nginx-1", job="app", instance="2"} 2+2x40
		http_requests_total{pod="nginx-2", job="app", instance="3"} 3+3x40`

	store := promqltest.LoadedStorage(t, load)
	defer store.Close()

	engineOpts := promql.EngineOpts{
		Timeout:    1 * time.Minute,
		MaxSamples: 1e10,
	}
	normalEngine := engine.New(engine.Opts{
		EngineOpts:        engineOpts,
		LogicalOptimizers: logicalplan.AllOptimizers,
	})
	projectionEngine := engine.New(engine.Opts{
		EngineOpts: engineOpts,
		LogicalOptimizers: []logicalplan.Optimizer{
			logicalplan.SortMatchers{},
			logicalplan.ProjectionOptimizer{},
			logicalplan.DetectHistogramStatsOptimizer{},
			logicalplan.MergeSelectsOptimizer{},
		},
	})

	projectionStorage := &noHashQueryable{Queryable: store}

	ctx := context.Background()
	queryTime := time.Unix(600, 0)
	queries := []string{
		`sum by (pod) (http_requests_total)`,
		`sum by (pod) (rate(http_requests_total[2m]))`,
		`sum without (instance) (abs(http_requests_total))`,
		`sum by (datacenter) (label_replace(http_requests_total, "datacenter", "$1", "pod", "^(nginx).*"))`,
		`sum by (datacenter) (label_replace(label_replace(http_requests_total, "datacenter", "$1", "pod", "^(nginx).*"), "datacenter", "$1", "pod", "^(nginx).*"))`,
		`sum by (datacenter) (label_join(http_requests_total, "datacenter", "-", "pod"))`,
		`sum by (pod) (timestamp(http_requests_total))`,
		`sum by (pod) (timestamp(abs(http_requests_total)))`,
		`sum by (pod) (timestamp(http_requests_total @ 600))`,
		`sum by (pod) (day_of_week(http_requests_total @ 600))`,
		`sum by (pod) (max_over_time(http_requests_total[2m:30s]))`,
		`sum by (pod) (abs(http_requests_total @ 600))`,
		`sum by (pod) (http_requests_total) / sum by (pod) (http_requests_total)`,
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			normalQuery, err := normalEngine.NewInstantQuery(ctx, store, &engine.QueryOpts{}, query, queryTime)
			testutil.Ok(t, err)
			defer normalQuery.Close()
			normalResult := normalQuery.Exec(ctx)
			testutil.Ok(t, normalResult.Err, "query: %s", query)

			projectionQuery, err := projectionEngine.MakeInstantQuery(ctx, projectionStorage, &engine.QueryOpts{}, query, queryTime)
			testutil.Ok(t, err)
			defer projectionQuery.Close()
			projectionResult := projectionQuery.Exec(ctx)
			testutil.Ok(t, projectionResult.Err, "query: %s", query)

			if diff := cmp.Diff(normalResult, projectionResult, comparer); diff != "" {
				t.Errorf("results differ for query %s: %s", query, diff)
			}
		})
	}
}
