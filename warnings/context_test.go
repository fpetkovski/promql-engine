// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package warnings_test

import (
	"context"
	"sync"
	"testing"

	"github.com/thanos-io/promql-engine/warnings"

	"github.com/efficientgo/core/errors"
	"github.com/efficientgo/core/testutil"
)

func TestConcurrentWarningSnapshots(t *testing.T) {
	ctx := warnings.NewContext(context.Background())
	warn := errors.New("warning")
	warnings.AddToContext(warn, ctx)
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 1000 {
			warnings.AddToContext(warn, ctx)
		}
	})
	for range 1000 {
		snapshot := warnings.FromContext(ctx)
		testutil.Equals(t, 1, len(snapshot))
		clear(snapshot)
	}
	wg.Wait()
	testutil.Equals(t, 1, len(warnings.FromContext(ctx)))
}
