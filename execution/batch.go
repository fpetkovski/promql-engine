// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package execution

import (
	"context"

	"github.com/thanos-io/promql-engine/execution/exchange"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	"github.com/thanos-io/promql-engine/storage"
	"github.com/thanos-io/promql-engine/warnings"

	"github.com/efficientgo/core/errors"
	promstorage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

// Batch holds roots and their shared-reader lifetimes. The caller must pull one
// batch from each active root per round, using contexts with a common lifetime.
// Detach each root on EOF or failure and Close the batch before closing scanners.
type Batch struct {
	Operators []model.VectorOperator
	Warnings  []annotations.Annotations
	releases  [][]func()
}

// NewBatch lowers boundaries selected by logicalplan.ShareCommonSubplans. The
// caller owns all scanners, including cleanup on construction failure.
func NewBatch(ctx context.Context, roots []logicalplan.Node, scanners []storage.Scanners, opts *query.Options) (*Batch, error) {
	if len(roots) != len(scanners) {
		return nil, errors.New("batch requires one scanner set per query")
	}
	shared := make(map[int]*exchange.Shared)
	counts := make(map[int]int)
	b := &Batch{
		Operators: make([]model.VectorOperator, len(roots)),
		Warnings:  make([]annotations.Annotations, len(roots)),
		releases:  make([][]func(), len(roots)),
	}
	for i, root := range roots {
		rootCtx := warnings.NewContext(ctx)
		var bind func(*logicalplan.Node) error
		bind = func(node *logicalplan.Node) error {
			if s, ok := (*node).(*logicalplan.Shared); ok {
				producer, found := shared[s.ID]
				if !found {
					producerCtx := warnings.NewContext(ctx)
					input, err := New(producerCtx, s.Expr, scanners[i], opts)
					if err != nil {
						return err
					}
					producer = exchange.NewShared(producerCtx, s.ID, input, s.Consumers, opts)
					shared[s.ID] = producer
					counts[s.ID] = s.Consumers
				}
				reader := producer.Reader()
				b.releases[i] = append(b.releases[i], reader.Close)
				counts[s.ID]--
				*node = &sharedReaderNode{Shared: s, reader: telemetry.NewOperator(reader.OperatorTelemetry, reader)}
				return nil
			}
			for _, child := range (*node).Children() {
				if err := bind(child); err != nil {
					return err
				}
			}
			return nil
		}
		root = root.Clone()
		if err := bind(&root); err != nil {
			return nil, err
		}
		operator, err := New(rootCtx, root, scanners[i], opts)
		if err != nil {
			return nil, err
		}
		b.Operators[i] = operator
		b.Warnings[i] = warnings.FromContext(rootCtx)
	}
	for _, remaining := range counts {
		if remaining != 0 {
			return nil, errors.New("shared plan consumer count does not match batch")
		}
	}
	return b, nil
}

func (b *Batch) Detach(index int) {
	for _, release := range b.releases[index] {
		release()
	}
	b.releases[index] = nil
}

func (b *Batch) Close() {
	for i := range b.Operators {
		b.Detach(i)
	}
}

// Bind explicit logical boundaries through the existing operator extension seam.
// Logical optimization never captures operators or execution factories.
type sharedReaderNode struct {
	*logicalplan.Shared
	reader model.VectorOperator
}

func (n *sharedReaderNode) MakeExecutionOperator(context.Context, *query.Options, promstorage.SelectHints) (model.VectorOperator, error) {
	return n.reader, nil
}
