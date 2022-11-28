package physicalplan

import (
	"github.com/thanos-community/promql-engine/logicalplan"
)

func DefaultOptimizers(numShards int) []logicalplan.Optimizer {
	return []logicalplan.Optimizer{
		ShardExpressions{TotalShards: numShards},
		InjectConcurrency{},
	}
}
