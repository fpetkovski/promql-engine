// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/thanos-io/promql-engine/api"
	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/promql-engine/query"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func TestExtendedSelectorsAgainstPrometheus(t *testing.T) {
	data := promqltest.LoadedStorage(t, `load 10s
		gauge 2 4 stale 8
		counter_total 0 10 2 8 14 20
		sparse_total _ _ _ 42
		hist_total {{schema:0 sum:2 count:2 buckets:[2]}} {{schema:0 sum:6 count:4 buckets:[4]}} {{schema:0 sum:1 count:1 buckets:[1]}} {{schema:0 sum:7 count:5 buckets:[5]}}
		mixed 1 {{schema:0 sum:2 count:2 buckets:[2]}} 3`)
	defer data.Close()

	ctx := context.Background()
	opts := engine.Opts{Timeout: time.Minute, MaxSamples: 1000, LookbackDelta: 30 * time.Second, DecodingConcurrency: 2}
	thanos, reference := engine.New(opts), promql.NewEngine(prometheusEngineOpts(opts))
	start, end, step := time.Unix(5, 0), time.Unix(60, 0), 5*time.Second
	for _, expr := range []string{
		`increase(counter_total[10s] anchored)`,
		`rate(counter_total[10s] smoothed)`,
		`changes(gauge[20s] anchored)`,         // No changes outside the nominal range.
		`increase(sparse_total[10s] smoothed)`, // No output before the first sample.
		`gauge smoothed offset 5s`,
		`gauge @ 15 smoothed`,
		`timestamp(gauge smoothed)`,
		`rate(hist_total[10s] anchored)`,
		`histogram_count(increase(hist_total[10s] smoothed))`,
		`hist_total smoothed`,
		`mixed smoothed`,
		`delta(mixed[10s] smoothed)`,
	} {
		t.Run(expr, func(t *testing.T) {
			q, err := thanos.NewRangeQuery(ctx, data, nil, expr, start, end, step)
			require.NoError(t, err)
			defer q.Close()
			ref, err := reference.NewRangeQuery(ctx, data, nil, expr, start, end, step)
			require.NoError(t, err)
			defer ref.Close()
			got, want := q.Exec(ctx), ref.Exec(ctx)
			require.NoError(t, got.Err)
			require.NoError(t, want.Err)
			if strings.Contains(expr, "mixed") {
				gw, _ := got.Warnings.AsStrings(expr, 0, 0)
				ww, _ := want.Warnings.AsStrings(expr, 0, 0)
				require.NotEmpty(t, ww)
				require.Equal(t, ww, gw)
			}
			testutil.WithGoCmp(comparer).Equals(t, want, got)
		})
	}
}

func TestExtendedSelectorFunctionRestrictions(t *testing.T) {
	data := promqltest.LoadedStorage(t, `load 10s
		m 1 2 3`)
	defer data.Close()
	ctx := context.Background()
	opts := engine.Opts{Timeout: time.Minute, MaxSamples: 1000}
	thanos, reference := engine.New(opts), promql.NewEngine(prometheusEngineOpts(opts))
	for _, expr := range []string{`absent_over_time(m[1m] anchored)`, `changes(m[1m] smoothed)`} {
		t.Run(expr, func(t *testing.T) {
			q, err := thanos.NewInstantQuery(ctx, data, nil, expr, time.Unix(20, 0))
			require.NoError(t, err)
			defer q.Close()
			ref, err := reference.NewInstantQuery(ctx, data, nil, expr, time.Unix(20, 0))
			require.NoError(t, err)
			defer ref.Close()
			want := ref.Exec(ctx).Err
			require.Error(t, want)
			require.EqualError(t, q.Exec(ctx).Err, want.Error())
		})
	}
}

func TestExtendedSelectorSampleLimit(t *testing.T) {
	data := promqltest.LoadedStorage(t, `load 10s
		m 1+1x24`)
	defer data.Close()
	ctx := context.Background()
	ng := engine.New(engine.Opts{Timeout: time.Minute, MaxSamples: 10})
	for _, expr := range []string{`m smoothed`, `delta(m[10s] anchored)`, `delta(m[10s] smoothed)`} {
		t.Run(expr, func(t *testing.T) {
			q, err := ng.NewInstantQuery(ctx, data, nil, expr, time.Unix(100, 0))
			require.NoError(t, err)
			defer q.Close()
			var limit query.ErrMaxSamplesExceeded
			require.ErrorAs(t, q.Exec(ctx).Err, &limit)
		})
	}
}

func TestExtendedSelectorStorageWindow(t *testing.T) {
	ctx := context.Background()
	ng := engine.New(engine.Opts{Timeout: time.Minute, MaxSamples: 1000, LookbackDelta: 30 * time.Second, DecodingConcurrency: 1})
	for _, tc := range []struct {
		expr              string
		start, end, width int64 // SelectHints in milliseconds.
	}{
		{`delta(m[20s] anchored offset 5s)`, 45001, 95000, 20000},
		{`delta(m[20s] smoothed offset 5s)`, 45001, 125000, 20000},
		{`m smoothed offset 5s`, 65001, 125000, 0},
	} {
		t.Run(tc.expr, func(t *testing.T) {
			var hints storage.SelectHints
			data := &storage.MockQueryable{MockQuerier: &storage.MockQuerier{
				SelectMockFunction: func(_ bool, h *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
					hints = *h
					return storage.EmptySeriesSet()
				},
			}}
			q, err := ng.NewInstantQuery(ctx, data, nil, tc.expr, time.Unix(100, 0))
			require.NoError(t, err)
			defer q.Close()
			require.NoError(t, q.Exec(ctx).Err)
			require.Equal(t, [3]int64{tc.start, tc.end, tc.width}, [3]int64{hints.Start, hints.End, hints.Range})
		})
	}
}

func TestDistributedSmoothedSelectorStaysLocal(t *testing.T) {
	ctx := context.Background()
	opts := engine.Opts{Timeout: time.Minute, MaxSamples: 1000, LookbackDelta: 120 * time.Second}
	older := promqltest.LoadedStorage(t, `load 10s
		x _ _ _ _ _ _ _ _ _ 0`)
	defer older.Close()
	complete := promqltest.LoadedStorage(t, `load 10s
		x _ _ _ _ _ _ _ _ _ 0 _ _ _ _ 10`)
	defer complete.Close()

	labelSets := []labels.Labels{labels.EmptyLabels()}
	remotes := []api.RemoteEngine{
		engine.NewRemoteEngine(opts, older, 0, 130_000, labelSets),
		engine.NewRemoteEngine(opts, complete, 10_000, 310_000, labelSets),
	}
	for _, n := range []int{1, 2} {
		// With one remote, passthrough would send the query to the older shard;
		// with both remotes, distributed execution would exclude the newer one.
		endpoints := api.NewStaticEndpoints(remotes[:n])
		q, err := engine.NewDistributedEngine(opts).MakeInstantQuery(ctx, complete, endpoints, nil, `x smoothed`, time.Unix(100, 0))
		require.NoError(t, err)
		defer q.Close()
		vec, err := q.Exec(ctx).Vector()
		require.NoError(t, err)
		require.Len(t, vec, 1)
		require.Equal(t, float64(2), vec[0].F)
	}
}
