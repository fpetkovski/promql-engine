package physicalplan

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-community/promql-engine/logicalplan"
)

func TestDefaultOptimizers(t *testing.T) {
	cases := []struct {
		name     string
		expr     string
		expected string
	}{
		{
			name: "shard matrix selector",
			expr: "stddev_over_time(http_requests_total[30s])",
			expected: `
concurrent(coalesce(
  concurrent(shard(stddev_over_time(http_requests_total[30s]), shard 0 of 3), 2), 
  concurrent(shard(stddev_over_time(http_requests_total[30s]), shard 1 of 3), 2), 
  concurrent(shard(stddev_over_time(http_requests_total[30s]), shard 2 of 3), 2)
), 2)
`,
		},
		{
			name: "nested function call",
			expr: `clamp(irate(http_requests_total[30s]), 10 - 5, 10)`,
			expected: `
concurrent(clamp(concurrent(coalesce(
  concurrent(shard(irate(http_requests_total[30s]), shard 0 of 3), 2), 
  concurrent(shard(irate(http_requests_total[30s]), shard 1 of 3), 2), 
  concurrent(shard(irate(http_requests_total[30s]), shard 2 of 3), 2)
), 2), 
concurrent(concurrent(10, 2) - concurrent(5, 2), 2), concurrent(10, 2)), 2)`,
		},
		{
			name: "shard aggregate",
			expr: `sum(metric{a="b", c="d"})`,
			expected: `
concurrent(sum(concurrent(coalesce(
	concurrent(sum(shard(metric{a="b",c="d"}, shard 0 of 3)), 2), 
	concurrent(sum(shard(metric{a="b",c="d"}, shard 1 of 3)), 2), 
	concurrent(sum(shard(metric{a="b",c="d"}, shard 2 of 3)), 2)
), 2)), 2)`,
		},
		{
			name: "shard aggregate",
			expr: `sum by (a) (metric{a="b", c="d"})`,
			expected: `
concurrent(sum by (a) (concurrent(coalesce(
	concurrent(shard(metric{a="b",c="d"}, shard 0 of 3), 2), 
	concurrent(shard(metric{a="b",c="d"}, shard 1 of 3), 2),
	concurrent(shard(metric{a="b",c="d"}, shard 2 of 3), 2)
), 2)), 2)`,
		},
		{
			name: "shard aggregate with matrix selector",
			expr: `sum(rate(metric{a="b",c="d"}[2m]))`,
			expected: `
concurrent(sum(concurrent(coalesce(
  concurrent(sum(shard(rate(metric{a="b",c="d"}[2m]), shard 0 of 3)), 2),
  concurrent(sum(shard(rate(metric{a="b",c="d"}[2m]), shard 1 of 3)), 2), 
  concurrent(sum(shard(rate(metric{a="b",c="d"}[2m]), shard 2 of 3)), 2)
), 2)), 2)`,
		},
		{
			name: "histogram_quantile",
			expr: `histogram_quantile(0.9, metric)`,
			expected: `
concurrent(histogram_quantile(
  concurrent(0.9, 2),
  concurrent(coalesce(
 	concurrent(shard(metric, shard 0 of 3), 2), 
	concurrent(shard(metric, shard 1 of 3), 2), 
	concurrent(shard(metric, shard 2 of 3), 2)
  ), 2)), 2)`,
		},
		{
			name: "aggregation with selector in param",
			expr: `quantile by (pod) (scalar(min(http_requests_total)), http_requests_total)`,
			expected: `
concurrent(quantile by (pod) (
  concurrent(scalar(concurrent(min(concurrent(coalesce(
    concurrent(shard(http_requests_total, shard 0 of 3), 2), 
    concurrent(shard(http_requests_total, shard 1 of 3), 2), 
    concurrent(shard(http_requests_total, shard 2 of 3), 2)
  ), 2)), 2)), 2), 
  concurrent(coalesce(
    concurrent(shard(http_requests_total, shard 0 of 3), 2), 
    concurrent(shard(http_requests_total, shard 1 of 3), 2), 
    concurrent(shard(http_requests_total, shard 2 of 3), 2)
  ), 2)), 2)`,
		},
	}

	spaces := regexp.MustCompile(`\s+`)
	openParenthesis := regexp.MustCompile(`\(\s+`)
	closedParenthesis := regexp.MustCompile(`\s+\)`)
	replacements := map[string]*regexp.Regexp{
		" ": spaces,
		"(": openParenthesis,
		")": closedParenthesis,
	}
	for _, tcase := range cases {
		t.Run(tcase.name, func(t *testing.T) {
			expr, err := parser.ParseExpr(tcase.expr)
			testutil.Ok(t, err)

			optimizers := DefaultOptimizers(3)
			plan := logicalplan.New(expr, time.Unix(0, 0), time.Unix(0, 0))

			optimizedPlan := plan.Optimize(optimizers)
			expectedPlan := cleanUp(replacements, tcase.expected)
			testutil.Equals(t, expectedPlan, optimizedPlan.Expr().String())
		})
	}
}

func cleanUp(replacements map[string]*regexp.Regexp, expr string) string {
	for replacement, match := range replacements {
		expr = match.ReplaceAllString(expr, replacement)
	}
	return strings.Trim(expr, " ")
}
