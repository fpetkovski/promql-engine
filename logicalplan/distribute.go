// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-community/promql-engine/api"
)

type RemoteExecutions []RemoteExecution

func (rs RemoteExecutions) String() string {
	parts := make([]string, len(rs))
	for i, r := range rs {
		parts[i] = r.String()
	}
	return strings.Join(parts, ", ")
}

// RemoteExecution is a logical plan that describes a
// remote execution of a Query against the given PromQL Engine.
type RemoteExecution struct {
	Engine          api.RemoteEngine
	Query           string
	QueryRangeStart time.Time
}

func (r RemoteExecution) String() string {
	if r.QueryRangeStart.UnixMilli() == 0 {
		return fmt.Sprintf("remote(%s)", r.Query)
	}
	return fmt.Sprintf("remote(%s) [%s]", r.Query, r.QueryRangeStart.String())
}

func (r RemoteExecution) Pretty(level int) string { return r.String() }

func (r RemoteExecution) PositionRange() parser.PositionRange { return parser.PositionRange{} }

func (r RemoteExecution) Type() parser.ValueType { return parser.ValueTypeMatrix }

func (r RemoteExecution) PromQLExpr() {}

// Deduplicate is a logical plan which deduplicates samples from multiple RemoteExecutions.
type Deduplicate struct {
	Expressions RemoteExecutions
}

func (r Deduplicate) String() string {
	return fmt.Sprintf("dedup(%s)", r.Expressions.String())
}

func (r Deduplicate) Pretty(level int) string { return r.String() }

func (r Deduplicate) PositionRange() parser.PositionRange { return parser.PositionRange{} }

func (r Deduplicate) Type() parser.ValueType { return parser.ValueTypeMatrix }

func (r Deduplicate) PromQLExpr() {}

// distributiveAggregations are all PromQL aggregations which support
// distributed execution.
var distributiveAggregations = map[parser.ItemType]struct{}{
	parser.SUM:     {},
	parser.MIN:     {},
	parser.MAX:     {},
	parser.GROUP:   {},
	parser.COUNT:   {},
	parser.BOTTOMK: {},
	parser.TOPK:    {},
}

// DistributedExecutionOptimizer produces a logical plan suitable for
// distributed Query execution.
type DistributedExecutionOptimizer struct {
	Endpoints api.RemoteEndpoints
}

func (m DistributedExecutionOptimizer) Optimize(plan parser.Expr, opts *Opts) parser.Expr {
	engines := m.Endpoints.Engines()
	traverseBottomUp(nil, &plan, func(parent, current *parser.Expr) (stop bool) {
		// If the current operation is not distributive, stop the traversal.
		if !isDistributive(current, parent) {
			return true
		}

		// If the current node is an aggregation, distribute the operation and
		// stop the traversal.
		if aggr, ok := (*current).(*parser.AggregateExpr); ok {
			localAggregation := aggr.Op
			if aggr.Op == parser.COUNT {
				localAggregation = parser.SUM
			}

			remoteAggregation := newRemoteAggregation(aggr, engines)
			subQueries := m.distributeQuery(&remoteAggregation, engines, opts)
			*current = &parser.AggregateExpr{
				Op:       localAggregation,
				Expr:     subQueries,
				Param:    aggr.Param,
				Grouping: aggr.Grouping,
				Without:  aggr.Without,
				PosRange: aggr.PosRange,
			}
			return true
		}

		// If the parent operation is distributive, continue the traversal.
		if isDistributive(parent, nil) {
			return false
		}

		*current = m.distributeQuery(current, engines, opts)
		return true
	})

	return plan
}

func newRemoteAggregation(rootAggregation *parser.AggregateExpr, engines []api.RemoteEngine) parser.Expr {
	groupingSet := make(map[string]struct{})
	for _, lbl := range rootAggregation.Grouping {
		groupingSet[lbl] = struct{}{}
	}

	for _, engine := range engines {
		for _, lbls := range engine.LabelSets() {
			for _, lbl := range lbls {
				if rootAggregation.Without {
					delete(groupingSet, lbl.Name)
				} else {
					groupingSet[lbl.Name] = struct{}{}
				}
			}
		}
	}

	groupingLabels := make([]string, 0, len(groupingSet))
	for lbl := range groupingSet {
		groupingLabels = append(groupingLabels, lbl)
	}
	sort.Strings(groupingLabels)

	remoteAggregation := *rootAggregation
	remoteAggregation.Grouping = groupingLabels
	return &remoteAggregation
}

// distributeQuery takes a PromQL expression in the form of *parser.Expr and a set of remote engines.
// For each engine which matches the time range of the query, it creates a RemoteExecution scoped to the range of the engine.
// All remote executions are wrapped in a Deduplicate logical node to make sure that results from overlapping engines are deduplicated.
// TODO(fpetkovski): Prune remote engines based on external labels.
func (m DistributedExecutionOptimizer) distributeQuery(expr *parser.Expr, engines []api.RemoteEngine, opts *Opts) Deduplicate {
	var selectRange time.Duration
	var offset time.Duration
	parser.Inspect(*expr, func(node parser.Node, nodes []parser.Node) error {
		if matrixSelector, ok := node.(*parser.MatrixSelector); ok {
			selectRange = matrixSelector.Range
		}
		if vectorSelector, ok := node.(*parser.VectorSelector); ok {
			offset = vectorSelector.Offset
		}
		return nil
	})
	offset = offset + selectRange

	var globalMaxT int64 = math.MinInt64
	for _, e := range engines {
		if e.MaxT() > globalMaxT {
			globalMaxT = e.MaxT()
		}
	}

	remoteQueries := make(RemoteExecutions, 0, len(engines))
	for _, e := range engines {
		start, keep := getStartTimeForEngine(e, opts, offset, globalMaxT)
		if !keep {
			continue
		}

		remoteQueries = append(remoteQueries, RemoteExecution{
			Engine:          e,
			Query:           (*expr).String(),
			QueryRangeStart: start,
		})
	}

	return Deduplicate{
		Expressions: remoteQueries,
	}
}

func getStartTimeForEngine(e api.RemoteEngine, opts *Opts, selectRange time.Duration, globalMaxT int64) (time.Time, bool) {
	if e.MinT() > opts.End.UnixMilli() {
		return time.Time{}, false
	}

	// A remote engine needs to have sufficient scope to do a lookback from the start of the query range.
	engineMinTime := time.UnixMilli(e.MinT())
	lookback := maxDuration(selectRange, opts.LookbackDelta)
	requiredMinTime := opts.Start.Add(-lookback)

	// Do not adjust the start time for instant queries since it would lead to changing
	// the user-provided timestamp and sending a result for a different time.
	if opts.IsInstantQuery() {
		return opts.Start, e.MaxT() != globalMaxT || engineMinTime.Before(requiredMinTime)
	}

	// If an engine's min time is before the start time of the query,
	// scope the query to the engine to the start of the range + the necessary
	// lookback duration.
	if e.MaxT() == globalMaxT && engineMinTime.After(requiredMinTime) {
		engineMinTime = calculateStepAlignedStart(opts, time.UnixMilli(e.MinT()).Add(lookback))
	}

	return maxTime(engineMinTime, opts.Start), true
}

// calculateStepAlignedStart returns a start time for the query based on the
// engine min time and the query step size.
// The purpose of this alignment is to make sure that the steps for the remote query
// have the same timestamps as the ones for the central query.
func calculateStepAlignedStart(opts *Opts, engineMinTime time.Time) time.Time {
	originalSteps := numSteps(opts.Start, opts.End, opts.Step)
	remoteQuerySteps := numSteps(engineMinTime, opts.End, opts.Step)

	stepsToSkip := originalSteps - remoteQuerySteps
	stepAlignedStartTime := opts.Start.UnixMilli() + stepsToSkip*opts.Step.Milliseconds()

	return time.UnixMilli(stepAlignedStartTime)
}

func numSteps(start, end time.Time, step time.Duration) int64 {
	return (end.UnixMilli()-start.UnixMilli())/step.Milliseconds() + 1
}

func isDistributive(expr *parser.Expr, parent *parser.Expr) bool {
	var offset time.Duration
	if expr != nil {
		if vectorSelector, ok := (*expr).(*parser.VectorSelector); ok {
			offset = vectorSelector.Offset
		}
	}
	var selectRange time.Duration
	if parent != nil {
		if matrixSelector, ok := (*parent).(*parser.MatrixSelector); ok {
			selectRange = matrixSelector.Range
		}
	}
	if offset+selectRange > 2*time.Hour+30*time.Minute {
		return false
	}

	if expr == nil {
		return false
	}
	switch aggr := (*expr).(type) {
	case *parser.BinaryExpr:
		// Binary expressions are joins and need to be done across the entire
		// data set. This is why we cannot push down aggregations where
		// the operand is a binary expression.
		// The only exception currently is pushing down binary expressions with a constant operand.
		lhsConstant := isNumberLiteral(aggr.LHS)
		rhsConstant := isNumberLiteral(aggr.RHS)
		return lhsConstant || rhsConstant
	case *parser.AggregateExpr:
		// Certain aggregations are currently not supported.
		if _, ok := distributiveAggregations[aggr.Op]; !ok {
			return false
		}
	case *parser.Call:
		return len(aggr.Args) > 0
	}

	return true
}

func isNumberLiteral(expr parser.Expr) bool {
	if _, ok := expr.(*parser.NumberLiteral); ok {
		return true
	}

	stepInvariant, ok := expr.(*parser.StepInvariantExpr)
	if !ok {
		return false
	}

	return isNumberLiteral(stepInvariant.Expr)
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
