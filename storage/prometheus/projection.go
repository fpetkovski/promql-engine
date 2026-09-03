// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package prometheus

import (
	"fmt"
	"slices"
	"strings"

	"github.com/thanos-io/promql-engine/logicalplan"
)

func formatProjection(projection *logicalplan.Projection) string {
	if projection == nil || (!projection.Include && len(projection.Labels) == 0) {
		return ""
	}

	projectionType := "exclude"
	if projection.Include {
		projectionType = "include"
	}

	labels := slices.Clone(projection.Labels)
	slices.Sort(labels)
	return fmt.Sprintf(" [projection=%s(%s)]", projectionType, strings.Join(labels, ","))
}
