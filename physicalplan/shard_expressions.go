package physicalplan

import (
	"fmt"

	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/exp/slices"

	"github.com/thanos-community/promql-engine/logicalplan"
)

var shardableFunctions = []parser.ItemType{parser.SUM, parser.COUNT, parser.TOPK, parser.BOTTOMK}

type Shard struct {
	Index int
	Total int
}

func (s Shard) String() string {
	return fmt.Sprintf("shard %d of %d", s.Index, s.Total)
}

type ShardedVectorSelector struct {
	*parser.VectorSelector
	Shard Shard
}

func newShardedVectorSelector(s *parser.VectorSelector, i int, numShards int) *ShardedVectorSelector {
	return &ShardedVectorSelector{
		VectorSelector: s,
		Shard:          Shard{Index: i, Total: numShards},
	}
}

func (f ShardedVectorSelector) String() string {
	return fmt.Sprintf("shard(%s, %s)", f.VectorSelector, f.Shard)
}

type ShardedMatrixSelector struct {
	*parser.MatrixSelector
	Call     *parser.Call
	Function *parser.Function
	Shard    Shard
}

func newShardedMatrixSelector(s *parser.MatrixSelector, f *parser.Call, i int, numShards int) *ShardedMatrixSelector {
	return &ShardedMatrixSelector{
		MatrixSelector: s,
		Call:           f,
		Function:       f.Func,
		Shard:          Shard{Index: i, Total: numShards},
	}
}

func (f ShardedMatrixSelector) Type() parser.ValueType { return parser.ValueTypeVector }

func (f ShardedMatrixSelector) String() string {
	return fmt.Sprintf("shard(%s(%s), %s)", f.Function.Name, f.MatrixSelector, f.Shard)
}

type Coalesce struct {
	Expressions parser.Expressions
}

func (f Coalesce) String() string {
	return fmt.Sprintf("coalesce(%v)", f.Expressions.String())
}

func (f Coalesce) Pretty(level int) string { return f.String() }

func (f Coalesce) PositionRange() parser.PositionRange { return f.Expressions[0].PositionRange() }

func (f Coalesce) Type() parser.ValueType { return f.Expressions[0].Type() }

func (f Coalesce) PromQLExpr() {}

type ShardExpressions struct {
	TotalShards int
}

func (s ShardExpressions) Optimize(expr parser.Expr) parser.Expr {
	s.optimize(&expr, s.TotalShards)
	return expr
}

func (s ShardExpressions) optimize(expr *parser.Expr, totalShards int) {
	excludeMatrixSelector := func(node *parser.Expr) bool {
		_, isMatrixSelector := (*node).(*parser.MatrixSelector)
		return !isMatrixSelector
	}

	logicalplan.TraverseDFSWithPredicate(expr, excludeMatrixSelector, func(expr *parser.Expr) {
		switch t := (*expr).(type) {
		case *parser.VectorSelector:
			selectors := make([]parser.Expr, totalShards)
			for i := range selectors {
				selectors[i] = newShardedVectorSelector(t, i, totalShards)
			}
			*expr = &Coalesce{Expressions: selectors}
		case *parser.Call:
			if len(t.Args) != 1 {
				return
			}
			matrixSelector, ok := t.Args[0].(*parser.MatrixSelector)
			if !ok {
				return
			}

			selectors := make([]parser.Expr, totalShards)
			for i := range selectors {
				selectors[i] = newShardedMatrixSelector(matrixSelector, t, i, totalShards)
			}
			*expr = &Coalesce{Expressions: selectors}

		case *parser.AggregateExpr:
			// Skip grouping aggregations, sharding them is less efficient right now.
			if len(t.Grouping) != 0 {
				return
			}

			if !slices.Contains(shardableFunctions, t.Op) {
				return
			}

			switch e := t.Expr.(type) {
			case *Coalesce:
				aggregators := make([]parser.Expr, len(e.Expressions))
				for i := range aggregators {
					aggregators[i] = &parser.AggregateExpr{
						Op:       t.Op,
						Param:    t.Param,
						Expr:     e.Expressions[i],
						Grouping: t.Grouping,
						Without:  t.Without,
						PosRange: t.PosRange,
					}
				}
				*expr = &parser.AggregateExpr{
					Op:       t.Op,
					Param:    t.Param,
					Expr:     &Coalesce{Expressions: aggregators},
					Grouping: t.Grouping,
					Without:  t.Without,
					PosRange: t.PosRange,
				}
			}
		}
	})
}
