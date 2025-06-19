// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package exchange

import (
	"context"
	"fmt"
	"sync"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/model/labels"
)

type maybeStepVector struct {
	err        error
	stepVector []model.StepVector
}

type concurrencyOperator struct {
	once sync.Once
	next model.VectorOperator

	nextVector chan maybeStepVector
	buffer     []maybeStepVector
	bufferSize int
	head       int
}

func NewConcurrent(next model.VectorOperator, bufferSize int, opts *query.Options) model.VectorOperator {
	oper := &concurrencyOperator{
		next:       next,
		bufferSize: bufferSize,
		buffer:     make([]maybeStepVector, bufferSize),
		nextVector: make(chan maybeStepVector, bufferSize),
	}

	return telemetry.NewOperator(telemetry.NewTelemetry(oper, opts), oper)
}

func (c *concurrencyOperator) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{c.next}
}

func (c *concurrencyOperator) String() string {
	return fmt.Sprintf("[concurrent(buff=%v)]", c.bufferSize)
}

func (c *concurrencyOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	series, err := c.next.Series(ctx)
	if err != nil {
		return nil, err
	}
	return series, nil
}

func (c *concurrencyOperator) GetPool() *model.VectorPool {
	return c.next.GetPool()
}

func (c *concurrencyOperator) Next(ctx context.Context, _ []model.StepVector) ([]model.StepVector, error) {
	c.once.Do(func() {
		go c.pull(ctx)
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case v, ok := <-c.nextVector:
		if !ok {
			return nil, nil
		}
		if v.err != nil {
			return nil, v.err
		}
		return v.stepVector, nil
	}
}

func (c *concurrencyOperator) pull(ctx context.Context) {
	defer func() {
		close(c.nextVector)
	}()

	for {
		r, err := c.next.Next(ctx, c.buffer[c.head].stepVector)
		if r == nil && err == nil {
			return
		}
		c.buffer[c.head] = maybeStepVector{stepVector: r, err: err}

		select {
		case <-ctx.Done():
			c.buffer[c.head].err = ctx.Err()
			return
		case c.nextVector <- c.buffer[c.head]:
			c.head = (c.head + 1) % c.bufferSize
		}
	}
}
