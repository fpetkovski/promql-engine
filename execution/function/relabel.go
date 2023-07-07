// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/parser"
)

type relabelFunctionOperator struct {
	next     model.VectorOperator
	funcExpr *parser.Call
	model.OperatorTelemetry
}

func (o *relabelFunctionOperator) Analyze() (model.OperatorTelemetry, []model.ObservableVectorOperator) {
	o.SetName("[*relabelFunctionOperator]")
	next := make([]model.ObservableVectorOperator, 0, 1)
	if obsnext, ok := o.next.(model.ObservableVectorOperator); ok {
		next = append(next, obsnext)
	}
	return o, next
}

func (o *relabelFunctionOperator) Explain() (me string, next []model.VectorOperator) {
	return "[*relabelFunctionOperator]", []model.VectorOperator{}
}

func (o *relabelFunctionOperator) Series(ctx context.Context) model.LabelsIterator {
	return model.NewProcessingIterator(o.next.Series(ctx), func(l labels.Labels) labels.Labels {
		return l
	})
	//series := o.next.Series(ctx)
	//if err != nil {
	//	return err
	//}
	//
	//switch o.funcExpr.Func.Name {
	//case "label_join":
	//	err = o.loadSeriesForLabelJoin(series)
	//case "label_replace":
	//	err = o.loadSeriesForLabelReplace(series)
	//default:
	//	err = errors.Newf("invalid function name for relabel operator: %s", o.funcExpr.Func.Name)
	//}
	//return err
}

func (o *relabelFunctionOperator) GetPool() *model.VectorPool {
	return o.next.GetPool()
}

func (o *relabelFunctionOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	start := time.Now()
	next, err := o.next.Next(ctx)
	o.AddExecutionTimeTaken(time.Since(start))
	return next, err
}
