//go:build unix

// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine_test

import (
	"fmt"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/promql-engine/internal/teststorage"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/promql"
)

func BenchmarkRangeBatch(b *testing.B) {
	for _, native := range []bool{false, true} {
		b.Run(fmt.Sprintf("native=%t", native), func(b *testing.B) {
			var load strings.Builder
			load.WriteString("load 30s\n")
			metric, grouping := "requests_bucket", "job, le"
			for i := range 1000 {
				if native {
					metric, grouping = "requests", "job"
					fmt.Fprintf(&load, " requests{job=\"job-%d\",instance=\"%d\"} {{schema:0 count:3 sum:14 buckets:[1 2]}}+{{schema:0 count:3 sum:14 buckets:[1 2]}}x240\n", i%10, i)
				} else {
					for j, le := range []string{"1", "10", "+Inf"} {
						fmt.Fprintf(&load, " requests_bucket{job=\"job-%d\",instance=\"%d\",le=\"%s\"} 0+%dx240\n", i%10, i, le, j+1)
					}
				}
			}
			store := teststorage.LoadedStorage(b, load.String())
			defer store.Close()
			ng := engine.New(engine.Opts{Timeout: time.Minute, DecodingConcurrency: 1})
			start, end, step := time.Unix(300, 0), time.Unix(7200, 0), 30*time.Second
			for _, count := range []int{2, 4} {
				for _, common := range []bool{true, false} {
					queries := make([]string, count)
					for i := range queries {
						selector := metric
						if !common {
							selector += fmt.Sprintf("{job=\"job-%d\"}", i)
						}
						queries[i] = fmt.Sprintf("histogram_quantile(%g, sum by(%s)(rate(%s[5m])))", 0.90+float64(i)/100, grouping, selector)
					}
					for _, batch := range []bool{false, true} {
						b.Run(fmt.Sprintf("queries=%d/common=%t/batch=%t", count, common, batch), func(b *testing.B) {
							b.ReportAllocs()
							var before, after syscall.Rusage
							testutil.Ok(b, syscall.Getrusage(syscall.RUSAGE_SELF, &before))
							for b.Loop() {
								var results []*promql.Result
								if batch {
									q, err := ng.NewRangeBatch(b.Context(), store, nil, queries, start, end, step)
									testutil.Ok(b, err)
									results = q.Exec(b.Context())
									q.Close()
								} else {
									// Independent collectors run concurrently, rather than
									// giving the batch an artificial sequential baseline.
									qs := make([]promql.Query, count)
									results = make([]*promql.Result, count)
									for i, text := range queries {
										q, err := ng.NewRangeQuery(b.Context(), store, nil, text, start, end, step)
										testutil.Ok(b, err)
										qs[i] = q
									}
									var wg sync.WaitGroup
									for i, q := range qs {
										wg.Go(func() { results[i] = q.Exec(b.Context()) })
									}
									wg.Wait()
									for _, q := range qs {
										q.Close()
									}
								}
								for _, result := range results {
									testutil.Ok(b, result.Err)
								}
							}
							testutil.Ok(b, syscall.Getrusage(syscall.RUSAGE_SELF, &after))
							cpu := after.Utime.Nano() + after.Stime.Nano() - before.Utime.Nano() - before.Stime.Nano()
							b.ReportMetric(float64(cpu)/float64(b.N), "cpu-ns/op")
						})
					}
				}
			}
		})
	}
}
