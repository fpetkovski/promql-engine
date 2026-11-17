// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"testing"
	"time"

	"github.com/thanos-io/promql-engine/query"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/promql/parser"
)

func TestShareCommonSubplans(t *testing.T) {
	for _, tc := range []struct {
		name     string
		queries  []string
		expected []string
	}{
		{
			name:     "whole queries",
			queries:  []string{`sum(rate(m[5m]))`, `sum(rate(m[5m]))`},
			expected: []string{`shared[0](sum(rate(m[5m])))`, `shared[0](sum(rate(m[5m])))`},
		},
		{
			name:     "quantiles with different source positions",
			queries:  []string{`histogram_quantile(0.9, sum by (job, le) (rate(m[5m])))`, `histogram_quantile(0.999, sum by (job, le) (rate(m[5m])))`},
			expected: []string{`histogram_quantile(0.9, shared[0](sum by (job, le) (rate(m[5m]))))`, `histogram_quantile(0.999, shared[0](sum by (job, le) (rate(m[5m]))))`},
		},
		{
			name:     "multiple independent sharing groups",
			queries:  []string{`abs(sum(m))`, `-sum(m)`, `abs(sum(n))`, `-sum(n)`},
			expected: []string{`abs(shared[0](sum(m)))`, `-shared[0](sum(m))`, `abs(shared[1](sum(n)))`, `-shared[1](sum(n))`},
		},
		{
			name:     "canonical matcher order",
			queries:  []string{`abs(sum(m{job="a",instance="b"}))`, `-sum(m{instance="b",job="a"})`},
			expected: []string{`abs(shared[0](sum(m{instance="b",job="a"})))`, `-shared[0](sum(m{instance="b",job="a"}))`},
		},
		{
			name:     "no descent through read-ahead consumers",
			queries:  []string{`sum(rate(m[5m]))`, `max(rate(m[5m]))`},
			expected: []string{`sum(rate(m[5m]))`, `max(rate(m[5m]))`},
		},
		{
			name:     "no descent through initialization drain",
			queries:  []string{`count_values("value", m)`, `abs(m)`},
			expected: []string{`count_values("value", m)`, `abs(m)`},
		},
		{
			name:     "no nested boundaries",
			queries:  []string{`abs(sum(m))`, `abs(sum(m))`, `sum(m)`},
			expected: []string{`shared[0](abs(sum(m)))`, `shared[0](abs(sum(m)))`, `sum(m)`},
		},
		{
			name:     "different windows and offsets",
			queries:  []string{`abs(sum(rate(m[5m])))`, `-sum(rate(m[2m]))`, `abs(sum(rate(m[5m] offset 1m)))`},
			expected: []string{`abs(sum(rate(m[5m])))`, `-sum(rate(m[2m]))`, `abs(sum(rate(m[5m] offset 1m)))`},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := &query.Options{Start: time.Unix(0, 0), End: time.Unix(600, 0), Step: time.Minute, LookbackDelta: 5 * time.Minute}
			roots := make([]Node, len(tc.queries))
			for i, text := range tc.queries {
				ast, err := parser.NewParser(parser.Options{}).ParseExpr(text)
				testutil.Ok(t, err)
				plan, err := NewFromAST(ast, opts, PlanOptions{})
				testutil.Ok(t, err)
				plan, _ = plan.Optimize(DefaultOptimizers)
				roots[i] = plan.Root()
			}
			for i, root := range ShareCommonSubplans(roots) {
				testutil.Equals(t, tc.expected[i], root.String())
				data, err := Marshal(root)
				testutil.Ok(t, err)
				decoded, err := Unmarshal(data)
				testutil.Ok(t, err)
				testutil.Equals(t, root.String(), decoded.String())
			}
		})
	}
}
