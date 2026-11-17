// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
)

// Shared is a batch-local execution boundary. Readers share Expr's output, not
// its mutable iterator. Insert these only after ordinary logical optimization.
type Shared struct {
	ID        int
	Consumers int
	Expr      Node `json:"-"`
}

func (s *Shared) Clone() Node {
	return &Shared{ID: s.ID, Consumers: s.Consumers, Expr: s.Expr.Clone()}
}

func (s *Shared) Children() []*Node            { return []*Node{&s.Expr} }
func (s *Shared) String() string               { return fmt.Sprintf("shared[%d](%s)", s.ID, s.Expr) }
func (s *Shared) Type() NodeType               { return SharedNode }
func (s *Shared) ReturnType() parser.ValueType { return s.Expr.ReturnType() }

// ShareCommonSubplans finds exact common subplans across already optimized roots
// with identical query options. Each root gets at most one sharing boundary:
// consumer paths must pull one input batch per output batch, without prefetch.
// Unsupported paths remain ordinary trees; this is not arbitrary DAG execution.
func ShareCommonSubplans(roots []Node) []Node {
	roots = append([]Node(nil), roots...)
	ids := make(map[Node]int)
	keys := make(map[string]int)
	var intern func(Node) int
	intern = func(node Node) int {
		children := make([]int, 0, len(node.Children()))
		for _, child := range node.Children() {
			id := intern(*child)
			if id < 0 {
				return -1
			}
			children = append(children, id)
		}
		data, ok := sharedNodeData(node)
		if !ok {
			return -1
		}
		key := fmt.Sprintf("%s/%s/%v", node.Type(), data, children)
		id, found := keys[key]
		if !found {
			id = len(keys)
			keys[key] = id
		}
		ids[node] = id
		return id
	}
	counts := make(map[int]int)
	for i := range roots {
		roots[i] = roots[i].Clone()
		intern(roots[i])
		for node := roots[i]; node != nil; node = sharedInput(node) {
			if id, ok := ids[node]; ok && node.ReturnType() == parser.ValueTypeVector {
				counts[id]++
			}
		}
	}

	// Prefer the largest common subtree. Do not install nested boundaries: an
	// ancestor shared by two roots can cover a descendant counted for more roots.
	groups := make(map[int][]*Node)
	var order []int
	for i := range roots {
		for slot := &roots[i]; slot != nil; slot = sharedInputSlot(*slot) {
			id, ok := ids[*slot]
			if !ok || counts[id] < 2 || (*slot).ReturnType() != parser.ValueTypeVector {
				continue
			}
			if len(groups[id]) == 0 {
				order = append(order, id)
			}
			groups[id] = append(groups[id], slot)
			break
		}
	}
	sharedID := 0
	for _, id := range order {
		slots := groups[id]
		if len(slots) < 2 {
			continue
		}
		for _, slot := range slots {
			*slot = &Shared{ID: sharedID, Consumers: len(slots), Expr: *slot}
		}
		sharedID++
	}
	return roots
}

func sharedInput(node Node) Node {
	if slot := sharedInputSlot(node); slot != nil {
		return *slot
	}
	return nil
}

// This whitelist is an execution contract, not a general function classification.
// In particular, do not descend into aggregates, subqueries or step invariants.
func sharedInputSlot(node Node) *Node {
	switch n := node.(type) {
	case *CheckDuplicateLabels:
		return &n.Expr
	case *Unary:
		return &n.Expr
	case *FunctionCall:
		switch n.Func.Name {
		case "abs":
			return &n.Args[0]
		case "histogram_quantile":
			if _, ok := n.Args[0].(*NumberLiteral); ok {
				return &n.Args[1]
			}
		}
	}
	return nil
}

// Encode only local attributes; canonical child IDs supply the subtree identity.
// Source positions and spelling do not affect equality, but projections, ranges,
// offsets, histogram modes and the resolved @ timestamp do.
func sharedNodeData(node Node) ([]byte, bool) {
	var value any = node
	switch n := node.(type) {
	case *VectorSelector:
		clone := *n
		selector := *n.VectorSelector
		selector.PosRange = posrange.PositionRange{}
		selector.OriginalOffsetExpr = nil
		clone.VectorSelector = &selector
		value = &clone
	case *MatrixSelector:
		value = n.Range
	case *NumberLiteral:
		value = strconv.FormatUint(math.Float64bits(n.Val), 16)
	case *Aggregation, *Binary, *FunctionCall, *StringLiteral, *Subquery,
		*CheckDuplicateLabels, *StepInvariantExpr, *Unary:
	default:
		// Unknown/custom/remote nodes have no established equivalence contract.
		return nil, false
	}
	data, err := json.Marshal(value)
	return data, err == nil
}
