# A Vectorized, Parallel, and Distributed Query Engine for PromQL

*Thanos Community — architecture note / paper draft*

## Abstract

We describe the design of the Thanos PromQL engine, a from-scratch
reimplementation of the Prometheus query engine that keeps full PromQL
compatibility while substantially improving throughput on large queries. The
engine models a query as a tree of *vector operators* that exchange
*columnar step vectors* in a pull-based volcano/iterator style. Three ideas
underpin its performance. First, every operator declares the complete set of
series it will ever emit *before* any samples flow, which lets the engine
replace label-keyed map lookups with dense integer indexing and pre-size all
intermediate buffers exactly. Second, execution proceeds in fixed-size batches
of evaluation steps over caller-owned buffers that are recycled in place,
eliminating per-step allocation without a garbage-collected object pool.
Third, range functions such as `rate` and `*_over_time` are evaluated by
incremental sliding-window ring buffers that fold each sample into every step
it contributes to, turning per-step recomputation into an amortized single
pass. On top of this substrate the engine exposes two orthogonal axes of
parallelism, a logical-plan optimizer framework, and a distributed execution
mode in which aggregations are rewritten as a fan-out of remote sub-queries and
a local merge. On multi-core hardware these choices yield up to an order of
magnitude speedup over the reference engine for aggregation-heavy queries.

## 1. Introduction

PromQL is the query language of the Prometheus monitoring system. The reference
engine evaluates a query one time series at a time, materializing every
selected series across the full query range before reducing it. This is simple
and correct, but it scales poorly: memory grows with the number of selected
series, and evaluation is single-threaded.

The Thanos PromQL engine targets the same language and semantics but is designed
around three goals: (i) bounded, predictable memory that grows with the *width*
of a single evaluation step rather than the length of the query; (ii) the
ability to use all available CPU cores for a single query; and (iii) the
ability to push work down to remote leaf engines, each responsible for a
disjoint slice of the data, so that a query can be evaluated across a federation
of independent query engines. These leaf nodes are themselves PromQL query
engines exposing a query API allowing the coordinator to delegate entire
sub-query plans rather than fetching raw series (see Section 7). It is a
drop-in library with wide spec support. Unsupported expressions return a
sentinel error so callers can fall back to the reference engine.

## 2. Execution model

A query is compiled into a tree of operators, each implementing a single
interface:

```go
type VectorOperator interface {
    Next(ctx context.Context, buf []StepVector) (int, error)
    Series(ctx context.Context) ([]labels.Labels, error)
    Explain() (next []VectorOperator)
    fmt.Stringer
}
```

Evaluation is *pull-based*: the root operator repeatedly calls `Next` on its
children, which call `Next` on theirs, until the leaves (selectors) report
exhaustion. The unit of data exchange is the **step vector**, a single
timestamp together with the values of all series alive at that timestamp:

```go
type StepVector struct {
    T            int64
    SampleIDs    []uint64
    Samples      []float64
    HistogramIDs []uint64
    Histograms   []*histogram.FloatHistogram
}
```

This is a columnar, struct-of-arrays layout. Values are stored in parallel
arrays indexed by series identity (Section 3), and floats are kept separate from
native histograms so that a float-only query never allocates or touches
histogram data. Since most PromQL expressions are aggregations, the number
of samples shrinks monotonically as data flows from the selectors toward the
root. The engine therefore decodes and holds samples one step (or one small
batch of steps) at a time instead of expanding whole series into memory.

Operators fall into a handful of families: *selectors* (instant-vector and
range/matrix, plus scalar-literal and subquery), *aggregations*
(`sum`/`min`/`max`/`avg`/`count`/… and the heap-based `topk`/`bottomk`/`limitk`
family), *binary* operators, *functions*, *unary* negation, *step-invariant*
(evaluate a step-independent subexpression once and replay it), and *exchange*
operators used purely for flow control and parallelism (Section 5). A physical
planner (`execution.New`) walks the logical plan and instantiates the
corresponding operator for each node.

## 3. Series identity and memory management

### 3.1 Declaring series up front

The bedrock of the design is the `Series()` method. Before any sample is
produced, an operator can be asked for the labels of *every* series it will ever
emit. Selectors know this because they query storage, and every other operator
computes its output series by calling `Series()` on its children and applying
its own transformation (an aggregation collapses groups, a binary operator
matches the two sides, and so on). Evaluating `Series()` costs roughly one
evaluation step, and it pays for itself many times over.

The position of a series in the slice returned by `Series()` *is* its
identifier — a dense, zero-based `uint64`. Every downstream reference to that
series is then an integer index rather than a label-set hash and map lookup.
Concretely this lets the engine:

- allocate intermediate tables and output vectors exactly, by size;
- use flat slices instead of maps for grouping, binary matching, and
  deduplication; and
- run tight, branch-free inner loops with no per-sample map probing.

When operators are merged across shards or engines, local identifier spaces are
rebased into a global one by adding a per-source offset (an additive
re-indexing), never by re-hashing labels.

### 3.2 Batched steps and caller-owned buffers

`Next` fills a buffer of step vectors supplied *by the caller*, returning how
many it wrote. Evaluation advances a small batch of steps at a time (default
ten). The caller owns the buffer and reuses the same backing slabs across
successive `Next` calls; a step vector is recycled by truncating its slices to
zero length while preserving capacity. Output arrays are grown once to the exact
expected series count via size hints, so a query reaches a steady state in which
the hot loop performs essentially no allocation.

This contract is the engine's main defence against garbage-collector pressure.
In a garbage-collected runtime, allocating a fresh step vector on every step of
every operator would produce a flood of short-lived heap objects, and the
collection of that garbage surfaces as CPU cost and latency jitter on precisely
the queries that touch the most data. By threading a small set of caller-owned
buffers through the operator tree and reusing them in place, the engine keeps
per-step working memory on long-lived, reused backing arrays, so the collector
sees almost no allocation regardless of the query's length. Recycling is
therefore *structural* — a consequence of the caller-owns-buffer contract rather
than of an explicit free list. An earlier version of the engine instead recycled
step vectors through an object pool that every operator borrowed from and
returned to; that achieved the same goal but coupled each operator to the pool's
lifecycle, and it was replaced by the simpler caller-owns-buffer ("reader")
contract described here.

### 3.3 Memory limits

Because samples are streamed rather than fully materialized, the peak memory of
the reducing pipeline is governed by the width of a step batch, not the query
length. The main exception is range selectors, whose per-series sliding-window
buffers scale with the cardinality of the selection rather than the step batch
(Section 4). A query may still optionally enforce a `MaxSamples` limit: an atomic
sample tracker is incremented and decremented as samples enter and leave
operators, and the query aborts with a "too many samples" error if the live
count exceeds the limit.

## 4. Range evaluation and ring buffers

Range selectors (`metric[5m]`) and the functions over them are where naive
evaluation is most expensive, because consecutive evaluation steps look at
heavily overlapping windows of samples. The engine handles this with sliding
ring buffers rather than an explicit "aligner" component; alignment is
decentralized, arising from each operator advancing `currentStep` by the query
step and computing a `[mint, maxt]` window per step.

For each step of a range selector the scanner computes the window
`maxt = step − offset`, `mint = maxt − range`, and slides its ring buffer
forward: samples older than the new `mint` are dropped, samples already in the
window are retained, and only genuinely new samples are pulled from the chunk
iterator. A single carried-over "next" sample avoids re-seeking storage. This
reuse across steps is the core of efficient range evaluation.

The engine keeps two implementations behind one buffer interface:

- a **generic buffer** that stores the retained samples and hands the whole
  window to a function closure; and
- **incremental streaming buffers** for `rate`/`increase`/`delta` and the
  `*_over_time` aggregations, which never keep the full window. Instead they
  maintain, per step, a compact accumulator (or, for rate, the first sample, a
  running last sample, and the list of counter resets) and fold each incoming
  sample into every step whose window contains it. Sliding the window forward
  is a rotation of these per-step arrays.

The streaming variant converts an `O(samples × overlapping-steps)`
recomputation into an amortized single pass, and is selected by a heuristic:
when a window overlaps at most five evaluation steps (the common case for
dashboards using `$__rate_interval`), the streaming buffer is used; for very
wide windows the engine falls back to the generic buffer to avoid keeping a
large number of per-step accumulators. Instant-vector lookback and staleness are
handled analogously, by a memoized iterator that can peek the previous sample
and accept it only if it falls within the lookback delta.

**Limitation: buffering is amortized across time, not across series.** The
streaming optimization removes redundant work between the overlapping *steps* of
a single series, but each series slides its window independently and therefore
owns a distinct ring buffer. A range selector allocates one buffer per selected
series and keeps them all alive for the duration of the query; the series
dimension is batched only in the sense that groups of series are advanced
together per `Next` call, not in the sense that a series' buffer can be released
early. The peak memory of a range query is therefore bounded not by the
step-batch width but by *(number of selected series) × (window buffer size)*.
This is the engine's dominant memory-scaling weakness: a `rate` — or any
`*_over_time` — over a high-cardinality selector must hold as many ring buffers
in memory simultaneously as there are series in flight, even when the
aggregation directly above it collapses them to a handful of output series.
Interleaving selection with aggregation, so that a per-series buffer can be
retired as soon as that series has been fully consumed and folded into its
group, would break this coupling; it remains an open direction (Section 10).

## 5. Parallelism

The volcano model admits *exchange* operators that are indistinguishable from
ordinary operators to their parents — they respect the same `Next` contract —
but exist only to move or reshape the flow of data. The engine uses them for two
orthogonal kinds of parallelism.

**Inter-operator (pipeline) parallelism.** A *concurrent* exchange decouples a
producer operator from its consumer through a bounded channel. A background
goroutine calls the child's `Next` ahead of demand and enqueues filled buffers;
the consumer swaps buffer ownership without copying and returns emptied buffers
for reuse. Inserting a concurrent exchange above expensive stages (aggregations,
deduplication, remote calls, each scan shard) lets each stage of the pipeline
run on its own core, so an operator can begin work on step *n* while its child
computes step *n+1*.

**Intra-operator (data) parallelism.** Selectors are fanned out into several
shards, each reading a disjoint hash partition of the matching series, each
wrapped in its own concurrent exchange, and merged by a *coalesce* exchange. The
coalesce operator concatenates the shards' series spaces (rebasing identifiers
by a per-shard offset) and aligns their outputs on a common timestamp. This
parallelizes the most expensive part of many queries — decoding chunks from
storage — across cores. A *deduplicate* exchange plays a similar merging role
when the same series arrives from replicated sources, keeping the freshest
sample.

## 6. Query planning and optimization

A query is first parsed into a logical plan: a tree of typed nodes that is an
intermediate representation independent of both the parser AST and the physical
operators. Two properties of this IR are worth noting. First, every node can
render itself back to valid PromQL text; this is not cosmetic, because the
distributed mode ships subtrees to remote engines as PromQL. Second, optimizers
rewrite the tree in place through child-slot pointers, so an optimizer is a
simple visitor rather than an immutable-rebuild pass.

Optimizers implement a single `Optimize(plan, opts)` method and run in sequence.
Rather than adding wrapper nodes, most of them *decorate the selector node
itself* with fields the physical planner later reads. The default set includes:

- **sort-matchers**, which canonicalizes matcher order so later passes can
  assume it;
- **merge-selects**, which detects that one selector is a superset of another
  for the same metric and rewrites the narrower one to reuse the broader
  selection plus a residual filter, so storage is queried once;
- **detect-histogram-stats**, which recognizes when only histogram count/sum are
  needed and tells selectors to skip decoding full buckets; and
- **selector-batch-size**, which enables batched selects under batchable
  aggregations.

Opt-in optimizers add **propagate-matchers** (pushing label matchers across the
two sides of a binary operation to shrink both selections) and
**projection-pushdown** (Section 6.1). A separate *selector pool* deduplicates
identical storage selects within a plan — the engine's structural stand-in for
common-subexpression elimination, which it does not otherwise perform.

### 6.1 Projection pushdown

A PromQL query rarely uses every label of the series it selects: a
`sum by (pod) (rate(http_requests_total[5m]))` only ever needs the `pod` label,
yet a naive selector fetches and decodes the full label set of every series.
Projection pushdown computes, for each selector, the minimal set of labels the
query actually requires and attaches it to the selector node as a *projection* —
a list of label names plus an `include` flag (`include = true` means "keep only
these labels", `include = false` means "drop these labels", mirroring PromQL's
`by`/`without`).

The projection is derived by walking the plan top-down and letting each node
narrow the requirement it passes to its children:

- an **aggregation** passes down exactly its grouping labels (`by (…)` becomes an
  include-projection, `without (…)` an exclude-projection);
- **`topk`/`bottomk`/`limitk`/`limit_ratio`** clear the projection, because they
  must return the full label sets of the series they rank;
- a **binary operation** passes down the labels named in its `on(…)`/`ignoring(…)`
  matching clause (plus `group_left/right` include labels), computed per side and
  per matching cardinality;
- **`label_replace`/`label_join`** add their source labels to the requirement
  only when the destination label is itself needed, and **`histogram_quantile`**
  disables projection entirely because it needs the `le` bucket label;
- **`absent`/`absent_over_time`/`scalar`** need no labels at all.

At execution time the projection is lowered into a Prometheus `SelectHints`
(`ProjectionLabels` + `ProjectionInclude`) and handed to storage. It is purely a
*hint*: the reference Prometheus TSDB ignores it, but storage implementations
that can honor it (such as Thanos stores) return only the requested label
columns, cutting decode and transfer cost for wide series. A storage backend that
honors the hint must attach a synthetic `__series_hash__` label carrying a hash
of the series' *full* label set, because the engine still needs a stable series
identity to perform horizontal joins and deduplication after the other labels
have been dropped. Because projection changes what a select returns, it also
participates in the selector-pool cache key, so two selectors are only shared
when their projections agree.

The engine is extensible along two seams. Callers may inject custom optimizers
at construction. And a logical node may implement `MakeExecutionOperator`,
letting external code splice an entirely custom physical operator into the tree
— the mechanism the distributed mode itself uses.

## 7. Distributed execution

In distributed mode the engine evaluates a query across several remote leaf
engines, each responsible for an independent dataset (for example, disjoint sets
of external labels or time ranges). Each remote is a full PromQL query engine in
its own right, exposing a range-query API rather than a raw storage API; this is
precisely why the coordinator can hand it an entire sub-expression as PromQL and
receive an aggregated result, instead of pulling back raw series and reducing
them centrally. Distribution is realized as a *plan rewrite*, not a separate
execution path: the same engine runs a plan into which two extra
optimizers have been injected, and the remote calls appear as ordinary operators
behind the `MakeExecutionOperator` seam.

The rewrite turns a central aggregation into a fan-out of remote sub-aggregations
and one local aggregation. For example, with two remote engines,

```
sum(rate(http_request_total[4m]))
```

becomes

```
sum(
  dedup(
    sum(rate(http_request_total[4m])),   # remote engine 1
    sum(rate(http_request_total[4m])),   # remote engine 2
  )
)
```

The optimizer decides *where* to distribute by marking the highest nodes that
can be pushed down, and *whether* a node may be pushed down by its algebraic
properties:

- Aggregations that are naturally distributive (`sum`, `min`, `max`, `count`,
  `group`, `topk`, `bottomk`) are split into a remote aggregation feeding a
  local one (with `count` becoming a local `sum`).
- Aggregations that are *not* distributive (`avg`, `quantile`, `stddev`, …) can
  still be pushed *whole* when they preserve the partition labels that shard the
  data across engines — each engine then computes over disjoint groups. When
  they do not, the optimizer either declines to distribute or rewrites them:
  `avg` without partition labels is distributed as `sum/count`, because
  averaging per-engine averages is incorrect.
- `absent`/`absent_over_time` are distributed as a product of per-engine
  results, with one engine guaranteed to evaluate the expression so an
  all-missing result still yields `absent`.

Remote fragments are cloned per matching engine, filtered to those whose
external labels and time range overlap the query, and merged by a deduplicate
node. Time handling is careful: each remote query's start is pushed back far
enough to satisfy the largest range or lookback needed to compute its first
step, and remote start times are snapped onto the same step grid as the central
query so that step timestamps coincide across engines. A pass-through optimizer
short-circuits the common case where exactly one engine can answer the whole
query, replacing the plan with a single remote execution and no local merge.

## 8. Observability

Every operator can be wrapped by a telemetry decorator that records per-call
timing, series counts, and per-step sample statistics. When analysis is
disabled the decorator is a no-op, so `Explain()` (the operator tree) and
`Analyze()` (the same tree annotated with live measurements and
Prometheus-compatible sample statistics) impose no cost on ordinary queries.

## 9. Evaluation

Microbenchmarks against the reference engine on an 8-core machine show large
speedups on aggregation- and function-heavy range queries — commonly 70–95%
lower wall-clock time (e.g. grouped `sum`, many-to-one binary joins, `clamp`,
`sort`, nested functions), while simple bare selectors are comparable or
slightly slower because the vectorized machinery adds fixed overhead that only
pays off once real reduction or parallel decoding is involved. Memory per query
is dominated by step-batch width rather than query length, except for range
functions, whose per-series window buffers scale with selection cardinality
(Section 4); parallel execution raises absolute memory because each concurrent
shard needs independent decode buffers. The full benchmark tables are maintained
in the project README and the continuous benchmark dashboard.

## 10. Related work and discussion

The engine is a direct application of the volcano/iterator model to time-series
evaluation, combined with vectorized (columnar, batched) execution in the spirit
of modern analytical databases. Its distinctive points relative to the reference
PromQL engine are the up-front `Series()` contract that enables integer series
identity, the incremental streaming ring buffers for range functions, and the
treatment of distributed execution as an algebra-aware plan rewrite rather than
a bespoke coordinator.

Open directions include per-query CPU limits (the engine currently uses cores
liberally), a genuine common-subexpression-elimination pass (only structural
select deduplication exists today), a fuller cost-based physical planner, and
retiring range-selector window buffers as soon as their series are consumed so
that the memory of range functions over high-cardinality selections is bounded
by output rather than input cardinality (Section 4).
