package logicalplan

import (
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/query"
)

type Scanners interface {
	NewVectorSelector(opts *query.Options, hints storage.SelectHints, selector VectorSelector) (model.VectorOperator, error)
	NewMatrixSelector(opts *query.Options, hints storage.SelectHints, logicalNode MatrixSelector, call parser.Call) (model.VectorOperator, error)
}
