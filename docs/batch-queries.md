# Batch range queries

`Engine.NewRangeBatch` evaluates multiple range queries with a common start,
end, step, and query options. It reuses exact common subplans where consumers can
safely advance together. Existing single-query APIs are unchanged.

```go
batch, err := ng.NewRangeBatch(ctx, queryable, queryOpts, []string{
	`histogram_quantile(0.95, sum by(job, le)(rate(requests_bucket[5m])))`,
	`histogram_quantile(0.99, sum by(job, le)(rate(requests_bucket[5m])))`,
}, start, end, step)
if err != nil {
	return err
}
defer batch.Close()

results := batch.Exec(ctx) // []*promql.Result, in input order
for _, result := range results {
	if result.Err != nil {
		// Handle this query's error.
		continue
	}
	matrix, err := result.Matrix()
	// Use matrix and result.Warnings as with an ordinary range query.
	_, _ = matrix, err
}
```

Use `MakeRangeBatch` with `*engine.QueryOpts` for engine-specific options, such as
selector batch size and logical optimizers. A batch must contain at least one
query; its step must be at least one millisecond and its end cannot precede its
start. Only scalar and instant-vector expressions are valid, as for ordinary
range queries. Construction is atomic: an invalid or unsupported expression fails
the constructor and closes any scanners it opened.

## Ownership and errors

- Execute a batch once and always close it, including if it is never executed.
  `Close` is idempotent. `Cancel` and `Close` may run concurrently with `Exec`;
  closing cancels execution and waits for its driver before closing scanners.
- `MaxSamples` is **batch-wide**, not a separate allowance per query. Existing
  operator state, the retained shared output batch, transient reader copies,
  and all retained result samples use the same tracker. Histogram samples are
  weighted by their size, as in single-query execution. This is a sample budget,
  not a byte-accurate heap limit; labels, buffer capacity and other metadata are
  not fully accounted for.
- `Timeout` covers the entire execution, including query-tracker admission.
  Canceling the construction context, the execution context, or the batch stops
  all members. Cancellation and sample-budget exhaustion return errors with no
  partial values for **all** members, including ones which already finished.
- An ordinary execution error fails its query. A shared producer error fails
  its dependent queries. Failed readers detach so surviving queries can finish.
  Shared warnings reach each reader; private warnings stay private.
- A batch occupies **one** active-query-tracker slot. Query metrics still count
  its individual expressions. Independent producers retain the ordinary decoding
  concurrency and bounded prefetch buffers; admission is not a global goroutine
  or heap bound. Callers should bound the number of queries they submit.
- Storage requirements are the same as for single-query execution. Engine-supplied
  scanners (`NewWithScanners`) are closed once per batch, not once per member.

## Sharing and execution

After each query's ordinary logical optimization, `ShareCommonSubplans` interns
subtrees bottom-up. Equality includes matcher sets, projections, offsets, ranges,
resolved `@` timestamps, and histogram decoding modes. Source positions and
spelling are not part of equality. Unknown/custom/remote nodes are not interned.

The initial implementation admits one sharing boundary per root and chooses the
largest eligible repeated subtree. Several independent sharing groups can coexist
in a batch. Identical complete vector-valued queries can share their entire
producer. Partial sharing is admitted only through audited consumer paths:
duplicate-label checks, unary operators, `abs`, and `histogram_quantile` with a
constant scalar argument. It does not descend through aggregation lookahead,
subquery expansion, step invariants, or initialization-time draining. Unsupported
paths execute as ordinary trees under the batch driver, not as shared DAGs.

The driver pulls one output batch from each active root per round. Each shared
producer retains one `[]StepVector` batch until every active reader has copied it.
Readers reuse caller-owned float buffers and deep-copy histograms. They never
expose the producer's mutable slices or histograms. No full-range cache of shared
output or decoded source samples is introduced. Existing producer state and
prefetch remain governed by their ordinary operators; result matrices still grow
with the requested output range.

The shared exchange rejects an invalid read-ahead schedule rather than retaining
another batch or waiting indefinitely. The allowlist and the driver form one
execution contract: expanding the allowlist requires checking both `Series` and
`Next`, including compiler-inserted prefetch.

`Explain()` returns one physical tree per query, with shared producer IDs.
`Analyze()` describes physical batch work: a shared producer appears under its
first occurrence only, so summing the trees does not double-count it. These are
not estimates of what each query would cost independently. Shared logical nodes
round-trip through the logical-plan codec, but their IDs are local to a complete
batch and are not executable as standalone single-query plans.

## Validation and performance

Tests cover result equivalence with single-query execution, logical and physical
plans, one storage scan per producer, warning isolation, histogram ownership,
reader detachment, producer failures/panics, cancellation, admission with a single
tracker slot, scanner cleanup, and batch-wide sample limits.

```sh
go test -race ./... -timeout=10m
go test ./engine -run '^$' -bench '^BenchmarkRangeBatch$' \
  -benchtime=10x -count=5 -benchmem -cpu=4
```

The benchmark compares the public batch API with independent queries executed
**concurrently**, including construction and result collection. It covers classic
and native histograms, two and four consumers, and no-common-subplan controls.
On an Apple M3 Pro with Go 1.26.7 and GOMAXPROCS 4, five isolated ten-iteration
runs gave these medians for the common-subplan workloads:

| Workload               | CPU: independent / batch | Allocated: independent / batch | Elapsed: independent / batch |
|------------------------|-------------------------:|-------------------------------:|-----------------------------:|
| Two classic quantiles  |        103.18 / 52.46 ms |                16.94 / 8.57 MB |             47.92 / 48.19 ms |
| Four classic quantiles |        249.01 / 57.17 ms |                33.88 / 8.72 MB |             66.56 / 52.11 ms |
| Two native quantiles   |        172.08 / 79.89 ms |               99.03 / 51.27 MB |             64.12 / 58.19 ms |
| Four native quantiles  |        365.47 / 79.05 ms |              198.05 / 53.89 MB |             94.48 / 56.86 ms |

The data has 1,000 instances, ten jobs and 241 samples per series (three bucket
series per instance for classic histograms). These are synthetic measurements,
not production guarantees or peak/live heap measurements.

Batching is intended to save duplicated work, not to guarantee lower latency.
With no common subplans, scheduling and planning add overhead. A longer classic
no-sharing control (two queries, five one-second runs) used 11.91 / 12.11 ms CPU,
1.775 / 1.787 MB allocated, and 5.32 / 6.42 ms elapsed for independent / batch
execution. Use ordinary independent queries when no sharing is expected and
latency is the priority. Different query grids, arbitrary DAGs, and sharing over
covering rather than identical evaluation contexts are not supported by this API.
