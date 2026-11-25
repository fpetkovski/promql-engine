// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

// SelectorMode describes how a selector evaluates the boundaries of its window.
type SelectorMode uint8

const (
	StandardSelector SelectorMode = iota
	AnchoredSelector
	SmoothedSelector
)

// Mode is the selector's effective mode. Direct timestamp() calls use the
// original sample timestamp, not an interpolated timestamp.
func (v *VectorSelector) Mode() SelectorMode {
	if !v.SelectTimestamp {
		switch {
		case v.Anchored:
			return AnchoredSelector
		case v.Smoothed:
			return SmoothedSelector
		}
	}
	return StandardSelector
}

// Window returns the amount of data, in milliseconds, needed before and
// after the reference time. Instant selectors have a zero-width nominal range.
func (m SelectorMode) Window(selectRange, lookback int64) (before, after int64) {
	before = selectRange
	if selectRange == 0 || m != StandardSelector {
		before += lookback
	}
	if m == SmoothedSelector {
		after = lookback
	}
	return before, after
}

func hasExtendedSelector(plan Node) bool {
	found := false
	Traverse(&plan, func(node *Node) {
		if vs, ok := (*node).(*VectorSelector); ok && vs.Mode() != StandardSelector {
			found = true
		}
	})
	return found
}
