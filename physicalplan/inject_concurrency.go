package physicalplan

import (
	"fmt"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-community/promql-engine/logicalplan"
)

type Concurrent struct {
	Expr   parser.Expr
	Buffer int
}

func NewConcurrent(expr parser.Expr, buffer int) parser.Expr {
	if _, ok := expr.(*Concurrent); ok {
		return expr
	}

	return &Concurrent{
		Expr: expr, Buffer: buffer,
	}
}

func (f Concurrent) String() string {
	return fmt.Sprintf("concurrent(%v, %d)", f.Expr.String(), f.Buffer)
}

func (f Concurrent) Pretty(level int) string { return f.String() }

func (f Concurrent) PositionRange() parser.PositionRange { return f.Expr.PositionRange() }

func (f Concurrent) Type() parser.ValueType { return f.Expr.Type() }

func (f Concurrent) PromQLExpr() {}

type InjectConcurrency struct{}

func (i InjectConcurrency) Optimize(expr parser.Expr) parser.Expr {
	excludeMatrixSelector := func(node *parser.Expr) bool {
		_, isMatrixSelector := (*node).(*parser.MatrixSelector)
		return !isMatrixSelector
	}

	logicalplan.TraverseDFSWithPredicate(&expr, excludeMatrixSelector, func(expr *parser.Expr) {
		switch e := (*expr).(type) {
		case *Coalesce:
			for i := range e.Expressions {
				e.Expressions[i] = NewConcurrent(e.Expressions[i], 2)
			}
		case *parser.AggregateExpr:
			e.Expr = NewConcurrent(e.Expr, 2)
			if e.Param != nil {
				e.Param = NewConcurrent(e.Param, 2)
			}
		case *parser.Call:
			for i := range e.Args {
				if _, ok := e.Args[i].(*parser.MatrixSelector); ok {
					continue
				}
				e.Args[i] = NewConcurrent(e.Args[i], 2)
			}
		case *parser.BinaryExpr:
			e.RHS = NewConcurrent(e.RHS, 2)
			e.LHS = NewConcurrent(e.LHS, 2)
		}
	})

	return NewConcurrent(expr, 2)
}
