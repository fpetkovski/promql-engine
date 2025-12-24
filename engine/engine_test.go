// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/pprof"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	"github.com/thanos-io/promql-engine/storage/prometheus"
	"github.com/thanos-io/promql-engine/warnings"

	"github.com/efficientgo/core/errors"
	"github.com/efficientgo/core/testutil"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"golang.org/x/exp/maps"
)

func TestMain(m *testing.M) {
	parser.EnableExperimentalFunctions = true
	goleak.VerifyTestMain(m,
		// https://github.com/census-instrumentation/opencensus-go/blob/d7677d6af5953e0506ac4c08f349c62b917a443a/stats/view/worker.go#L34
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	)
}

type skipTest struct {
	skipTests []string
	promqltest.TBRun
}

func (s *skipTest) Run(name string, t func(*testing.T)) bool {
	if slices.Contains(s.skipTests, name) {
		return true
	}

	return s.TBRun.Run(name, t)
}

func TestPromqlAcceptance(t *testing.T) {
	// promql acceptance tests disable experimental functions again
	// since we use them in our tests too we need to enable them afterwards again
	t.Cleanup(func() { parser.EnableExperimentalFunctions = true })

	engine := engine.New(engine.Opts{
		EngineOpts: promql.EngineOpts{
			Logger:                   promslog.NewNopLogger(),
			EnableAtModifier:         true,
			EnableNegativeOffset:     true,
			MaxSamples:               5e10,
			Timeout:                  1 * time.Hour,
			NoStepSubqueryIntervalFn: func(rangeMillis int64) int64 { return 30 * time.Second.Milliseconds() },
		}})

	st := &skipTest{
		skipTests: []string{
			"testdata/name_label_dropping.test", // feature unsupported
			"testdata/type_and_unit.test",       // feature unsupported
		}, // TODO(sungjin1212): change to test whole cases
		TBRun: t,
	}

	promqltest.RunBuiltinTests(st, engine)
}

func TestVectorSelectorWithGaps(t *testing.T) {
	t.Parallel()
	opts := promql.EngineOpts{
		Timeout:              1 * time.Hour,
		MaxSamples:           1e10,
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	}

	series := storage.MockSeries(
		[]int64{240, 270, 300, 600, 630, 660},
		[]float64{1, 2, 3, 4, 5, 6},
		[]string{labels.MetricName, "foo"},
	)

	query := "foo"
	start := time.Unix(0, 0)
	end := time.Unix(1000, 0)

	ctx := context.Background()
	newEngine := engine.New(engine.Opts{EngineOpts: opts})
	q1, err := newEngine.NewRangeQuery(ctx, storageWithSeries(series), nil, query, start, end, 30*time.Second)
	testutil.Ok(t, err)
	defer q1.Close()

	newResult := q1.Exec(ctx)
	testutil.Ok(t, newResult.Err)

	oldEngine := promql.NewEngine(opts)
	q2, err := oldEngine.NewRangeQuery(ctx, storageWithSeries(series), nil, query, start, end, 30*time.Second)
	testutil.Ok(t, err)
	defer q2.Close()

	oldResult := q2.Exec(context.Background())
	testutil.Ok(t, oldResult.Err)

	testutil.WithGoCmp(comparer).Equals(t, oldResult, newResult, queryExplanation(q1))
}

type queryableCloseChecker struct {
	closed bool

	storage.Queryable
}

func (q *queryableCloseChecker) Querier(mint, maxt int64) (storage.Querier, error) {
	qr, err := q.Queryable.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &querierCloseChecker{Querier: qr, closed: &q.closed}, nil
}

type querierCloseChecker struct {
	storage.Querier

	closed *bool
}

func (q *querierCloseChecker) Close() error {
	*q.closed = true
	return q.Querier.Close()
}

// TestQuerierClosedAfterQueryClosed tests that the querier is only closed
// after the query is closed.
func TestQuerierClosedAfterQueryClosed(t *testing.T) {
	t.Parallel()
	opts := promql.EngineOpts{
		Timeout:              1 * time.Hour,
		MaxSamples:           1e10,
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	}

	load := `load 30s
			    http_requests_total{pod="nginx-1", route="/"} 41.00+0.20x40
			    http_requests_total{pod="nginx-2", route="/"} 51+21.71x40`

	storage := promqltest.LoadedStorage(t, load)
	defer storage.Close()

	optimizers := logicalplan.AllOptimizers
	newEngine := engine.New(engine.Opts{
		EngineOpts:        opts,
		LogicalOptimizers: optimizers,
		// Set to 1 to make sure batching is tested.
		SelectorBatchSize: 1,
	})
	ctx := context.Background()
	qr := &queryableCloseChecker{
		Queryable: storage,
	}
	q1, err := newEngine.NewInstantQuery(ctx, qr, nil, "sum(http_requests_total)", time.Unix(0, 0))
	testutil.Ok(t, err)
	_ = q1.Exec(ctx)

	require.Equal(t, false, qr.closed)
	q1.Close()

	require.Equal(t, true, qr.closed)
}

func TestQueriesAgainstOldEngine(t *testing.T) {
	t.Parallel()
	start := time.Unix(0, 0)
	end := time.Unix(1800, 0)
	step := time.Second * 30
	// Negative offset and at modifier are enabled by default
	// since Prometheus v2.33.0 so we also enable them.
	opts := promql.EngineOpts{
		Timeout:              1 * time.Hour,
		MaxSamples:           1e10,
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
		NoStepSubqueryIntervalFn: func(rangeMillis int64) int64 {
			return 15 * time.Second.Milliseconds()
		},
	}

	// Load test cases from testdata directory
	testdataDir := filepath.Join("testdata")
	cases, err := loadTestFilesFromDir(testdataDir)
	if err != nil {
		t.Fatalf("failed to load test files: %v", err)
	}
	disableOptimizerOpts := []bool{true, false}
	lookbackDeltas := []time.Duration{0, 30 * time.Second, time.Minute, 5 * time.Minute, 10 * time.Minute}
	for _, lookbackDelta := range lookbackDeltas {
		opts.LookbackDelta = lookbackDelta
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				storage := promqltest.LoadedStorage(t, tc.load)
				defer storage.Close()

				if tc.start.Equal(time.Time{}) {
					tc.start = start
				}
				if tc.end.Equal(time.Time{}) {
					tc.end = end
				}
				if tc.step == 0 {
					tc.step = step
				}
				for _, disableOptimizers := range disableOptimizerOpts {
					t.Run(fmt.Sprintf("disableOptimizers=%v", disableOptimizers), func(t *testing.T) {
						optimizers := logicalplan.AllOptimizers
						if disableOptimizers {
							optimizers = logicalplan.NoOptimizers
						}
						newEngine := engine.New(engine.Opts{
							EngineOpts:        opts,
							LogicalOptimizers: optimizers,
							// Set to 1 to make sure batching is tested.
							SelectorBatchSize: 1,
						})
						ctx := context.Background()

						var q1, q2 promql.Query
						var err error

						oldEngine := promql.NewEngine(opts)

						// Use instant query if start == end and step == 0
						if tc.start.Equal(tc.end) && tc.step == 0 {
							q1, err = newEngine.NewInstantQuery(ctx, storage, nil, tc.query, tc.start)
							testutil.Ok(t, err)
							defer q1.Close()

							q2, err = oldEngine.NewInstantQuery(ctx, storage, nil, tc.query, tc.start)
							testutil.Ok(t, err)
							defer q2.Close()
						} else {
							q1, err = newEngine.NewRangeQuery(ctx, storage, nil, tc.query, tc.start, tc.end, tc.step)
							testutil.Ok(t, err)
							defer q1.Close()

							q2, err = oldEngine.NewRangeQuery(ctx, storage, nil, tc.query, tc.start, tc.end, tc.step)
							testutil.Ok(t, err)
							defer q2.Close()
						}

						newResult := q1.Exec(ctx)
						oldResult := q2.Exec(ctx)

						testutil.WithGoCmp(comparer).Equals(t, oldResult, newResult, queryExplanation(q1))
					})
				}
			})
		}
	}
}

// parseTestFile parses a Prometheus test file and extracts test cases for TestQueriesAgainstOldEngine.
// It extracts load blocks and eval range commands, using promqltest.LoadedStorage for parsing.
func parseTestFile(path string) ([]struct {
	load  string
	name  string
	query string
	start time.Time
	end   time.Time
	step  time.Duration
}, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// Split by "clear" to get blocks
	blocks := strings.Split(string(content), "\nclear\n")

	var cases []struct {
		load  string
		name  string
		query string
		start time.Time
		end   time.Time
		step  time.Duration
	}

	for _, block := range blocks {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}

		lines := strings.Split(block, "\n")
		var name, load, evalLine string
		var loadLines []string
		inLoad := false

		// Extract name (comment), load, and eval from this block
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)

			// Extract test name from comment
			if strings.HasPrefix(trimmed, "#") {
				name = strings.TrimSpace(strings.TrimPrefix(trimmed, "#"))
				continue
			}

			// Extract load block
			if strings.HasPrefix(trimmed, "load ") {
				inLoad = true
				loadLines = append(loadLines, line)
				continue
			}

			if inLoad && !strings.HasPrefix(trimmed, "eval ") {
				loadLines = append(loadLines, line)
				continue
			}

			// Extract eval line (might be multi-line) - handles both instant and range
			if strings.HasPrefix(trimmed, "eval range from ") || strings.HasPrefix(trimmed, "eval instant at ") {
				inLoad = false
				evalLine = trimmed
				continue
			}

			// Continuation of eval query (multi-line query)
			if evalLine != "" && trimmed != "" {
				evalLine += " " + trimmed
			}
		}

		if evalLine == "" {
			continue
		}

		// Reconstruct load block
		if len(loadLines) > 0 {
			load = strings.Join(loadLines, "\n")
		}

		var query string
		var startTime, endTime time.Time
		var step time.Duration

		// Parse eval line - handle both instant and range queries
		if strings.HasPrefix(evalLine, "eval instant at ") {
			// Parse: "eval instant at <time> <query>"
			evalLine = strings.TrimPrefix(evalLine, "eval instant at ")
			parts := strings.Fields(evalLine)

			if len(parts) < 2 {
				continue
			}

			timeStr := parts[0]
			query = strings.Join(parts[1:], " ")

			// For instant queries, start = end = queryTime
			t, _ := parseDuration(timeStr)
			startTime = time.Unix(int64(t.Seconds()), 0)
			endTime = startTime
			step = 0
		} else if strings.HasPrefix(evalLine, "eval range from ") {
			// Parse: "eval range from <start> to <end> step <step> <query>"
			evalLine = strings.TrimPrefix(evalLine, "eval range from ")
			parts := strings.Fields(evalLine)

			if len(parts) < 7 {
				continue
			}

			startStr := parts[0]
			// parts[1] is "to"
			endStr := parts[2]
			// parts[3] is "step"
			stepStr := parts[4]

			// Everything after "step <duration>" is the query
			query = strings.Join(parts[5:], " ")

			// Parse times
			s, _ := parseDuration(startStr)
			e, _ := parseDuration(endStr)
			step, _ = time.ParseDuration(stepStr)

			startTime = time.Unix(int64(s.Seconds()), 0)
			endTime = time.Unix(int64(e.Seconds()), 0)
		} else {
			continue
		}

		if name == "" {
			name = query
		}

		cases = append(cases, struct {
			load  string
			name  string
			query string
			start time.Time
			end   time.Time
			step  time.Duration
		}{
			name:  name,
			load:  load,
			query: query,
			start: startTime,
			end:   endTime,
			step:  step,
		})
	}

	return cases, nil
}

// parseDuration parses durations like "30m", "1h", "0" into time.Duration
func parseDuration(s string) (time.Duration, error) {
	if s == "0" {
		return 0, nil
	}
	return time.ParseDuration(s)
}

// loadTestFilesFromDir loads all .test files from a directory
func loadTestFilesFromDir(dir string) ([]struct {
	load  string
	name  string
	query string
	start time.Time
	end   time.Time
	step  time.Duration
}, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var allCases []struct {
		load  string
		name  string
		query string
		start time.Time
		end   time.Time
		step  time.Duration
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".test") {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		cases, err := parseTestFile(path)
		if err != nil {
			return nil, err
		}

		// Prefix test names with filename for uniqueness
		filePrefix := strings.TrimSuffix(entry.Name(), ".test")
		for i := range cases {
			cases[i].name = filePrefix + "/" + cases[i].name
		}

		allCases = append(allCases, cases...)
	}

	return allCases, nil
}

// mergeWithSampleDedup merges samples from series with the same labels,
// removing samples with identical timestamps.
func mergeWithSampleDedup(series []*mockSeries) []storage.Series {
	index := make(map[uint64]*mockSeries)
	for _, s := range series {
		hash := s.Labels().Hash()
		existing, ok := index[hash]
		if !ok {
			// Make a copy to avoid modifying the original series
			// when merging samples.
			index[hash] = &mockSeries{
				labels:     s.labels,
				timestamps: s.timestamps,
				values:     s.values,
			}
			continue
		}
		existing.timestamps = append(existing.timestamps, s.timestamps...)
		existing.values = append(existing.values, s.values...)
	}

	for _, s := range index {
		sort.Sort(byTimestamps(*s))
		// Remove exact timestamp duplicates.
		i := 1
		for i < len(s.timestamps) {
			if s.timestamps[i] == s.timestamps[i-1] {
				s.timestamps = slices.Delete(s.timestamps, i, i+1)
				s.values = slices.Delete(s.values, i, i+1)
			} else {
				i++
			}
		}
	}

	sset := make([]storage.Series, 0, len(index))
	for _, s := range index {
		sset = append(sset, s)
	}
	return sset
}

func TestWarnings(t *testing.T) {
	querier := &storage.MockQueryable{
		MockQuerier: &storage.MockQuerier{
			SelectMockFunction: func(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
				return newWarningsSeriesSet(annotations.New().Add(errors.New("test warning")))
			},
		},
	}

	var (
		start = time.UnixMilli(0)
		end   = time.UnixMilli(600)
		step  = 30 * time.Second
	)

	cases := []struct {
		name          string
		query         string
		expectedWarns annotations.Annotations
	}{
		{
			name:  "single select call",
			query: `http_requests_total`,
			expectedWarns: annotations.New().Add(
				errors.New("test warning"),
			),
		},
		{
			name:  "multiple select calls",
			query: `sum(http_requests_total) / sum(http_responses_total)`,
			expectedWarns: annotations.New().Add(
				errors.New("test warning"),
			),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			newEngine := engine.New(engine.Opts{EngineOpts: promql.EngineOpts{Timeout: 1 * time.Hour}})
			q1, err := newEngine.NewRangeQuery(context.Background(), querier, nil, tc.query, start, end, step)
			testutil.Ok(t, err)

			res := q1.Exec(context.Background())
			testutil.Ok(t, res.Err)
			testutil.WithGoCmp(cmp.Comparer(func(err1, err2 error) bool {
				return err1.Error() == err2.Error()
			})).Equals(t, tc.expectedWarns, res.Warnings)
		})
	}
}

type scannersWithWarns struct {
	warn         error
	promScanners *prometheus.Scanners
}

func newScannersWithWarns(warn error, qOpts *query.Options, lplan logicalplan.Plan) (*scannersWithWarns, error) {
	scanners, err := prometheus.NewPrometheusScanners(&storage.MockQueryable{
		MockQuerier: storage.NoopQuerier(),
	}, qOpts, lplan)
	if err != nil {
		return nil, err
	}
	return &scannersWithWarns{
		warn:         warn,
		promScanners: scanners,
	}, nil
}

func (s *scannersWithWarns) Close() error { return nil }

func (s scannersWithWarns) NewVectorSelector(ctx context.Context, opts *query.Options, hints storage.SelectHints, selector logicalplan.VectorSelector) (model.VectorOperator, error) {
	warnings.AddToContext(s.warn, ctx)
	return s.promScanners.NewVectorSelector(ctx, opts, hints, selector)
}

func (s scannersWithWarns) NewMatrixSelector(ctx context.Context, opts *query.Options, hints storage.SelectHints, selector logicalplan.MatrixSelector, call logicalplan.FunctionCall) (model.VectorOperator, error) {
	warnings.AddToContext(s.warn, ctx)
	return s.promScanners.NewMatrixSelector(ctx, opts, hints, selector, call)
}

func TestWarningsPlanCreation(t *testing.T) {
	var (
		opts         = engine.Opts{EngineOpts: promql.EngineOpts{Timeout: 1 * time.Hour}}
		expectedWarn = errors.New("test warning")
	)

	scnrs, err := newScannersWithWarns(expectedWarn, &query.Options{}, nil)
	testutil.Ok(t, err)
	newEngine := engine.NewWithScanners(opts, scnrs)
	q1, err := newEngine.NewRangeQuery(context.Background(), nil, nil, "http_requests_total", time.UnixMilli(0), time.UnixMilli(600), 30*time.Second)
	testutil.Ok(t, err)

	res := q1.Exec(context.Background())
	testutil.Ok(t, res.Err)
	testutil.WithGoCmp(cmp.Comparer(func(err1, err2 error) bool {
		return err1.Error() == err2.Error()
	})).Equals(t, annotations.New().Add(expectedWarn), res.Warnings)

}

func TestEdgeCases(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name   string
		series []storage.Series
		query  string
		start  time.Time
		end    time.Time
	}{
		{
			name: "binop edge case",
			series: []storage.Series{
				newMockSeries(
					[]string{labels.MetricName, "foo"},
					[]int64{0, 30, 60, 1200, 1500, 1800},
					[]float64{1, 2, 3, 4, 5, 6},
				),
				newMockSeries(
					[]string{labels.MetricName, "bar", "id", "1"},
					[]int64{0, 30},
					[]float64{1, 2},
				),
				newMockSeries(
					[]string{labels.MetricName, "bar", "id", "2"},
					[]int64{1200, 1500},
					[]float64{3, 4},
				),
			},
			query: `foo * on () group_left () bar`,
			start: time.Unix(0, 0),
			end:   time.Unix(30000, 0),
		},
		{
			name: "absent with gaps in series",
			series: []storage.Series{
				newMockSeries(
					[]string{labels.MetricName, "foo"},
					[]int64{30, 300, 3000, 6000, 12000, 18000},
					[]float64{1, 2, 3, 4, 5, 6},
				),
			},
			query: `absent(foo)`,
			start: time.Unix(0, 0),
			end:   time.Unix(30000, 0),
		},
	}

	opts := promql.EngineOpts{
		Timeout:              1 * time.Hour,
		MaxSamples:           1e10,
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	}
	step := time.Second * 30
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			oldEngine := promql.NewEngine(opts)
			q1, err := oldEngine.NewRangeQuery(ctx, storageWithSeries(tc.series...), nil, tc.query, tc.start, tc.end, step)
			testutil.Ok(t, err)

			newEngine := engine.New(engine.Opts{EngineOpts: opts})
			q2, err := newEngine.NewRangeQuery(ctx, storageWithSeries(tc.series...), nil, tc.query, tc.start, tc.end, step)
			testutil.Ok(t, err)

			oldResult := q1.Exec(ctx)
			newResult := q2.Exec(ctx)

			testutil.WithGoCmp(comparer).Equals(t, oldResult, newResult, queryExplanation(q1))
		})
	}
}

func TestXFunctionsRangeQuery(t *testing.T) {
	// Negative offset and at modifier are enabled by default
	// since Prometheus v2.33.0, so we also enable them.
	opts := promql.EngineOpts{
		Timeout:              1 * time.Hour,
		MaxSamples:           1e10,
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	}

	cases := []struct {
		name      string
		load      string
		query     string
		startTime time.Time
		endTime   time.Time
		step      time.Duration

		expected promql.Matrix
	}{
		{
			name: "gaps between steps",
			load: `load 10s
			    http_requests 1 5 10 20 _ 40`,
			query: "xincrease(http_requests[10s])",

			startTime: time.Unix(0, 0),
			endTime:   time.Unix(60, 0),
			step:      20 * time.Second,

			expected: promql.Matrix{
				promql.Series{
					Metric: labels.New(),
					Floats: []promql.FPoint{
						{T: 00_000, F: 1},
						{T: 20_000, F: 9}, // TODO: this seems odd, feels like it should be 5
						{T: 40_000, F: 0},
						{T: 60_000, F: 0},
					},
				},
			},
		},
		{
			name: "back to back steps",
			load: `load 10s
			    http_requests 1 5 10 20 _ 40`,
			query: "xincrease(http_requests[10s])",

			startTime: time.Unix(0, 0),
			endTime:   time.Unix(60, 0),
			step:      10 * time.Second,

			expected: promql.Matrix{
				promql.Series{
					Metric: labels.New(),
					Floats: []promql.FPoint{
						{T: 00_000, F: 1},
						{T: 10_000, F: 4},
						{T: 20_000, F: 5},
						{T: 30_000, F: 10},
						{T: 40_000, F: 0},
						{T: 50_000, F: 20},
						{T: 60_000, F: 0},
					},
				},
			},
		},
		{
			name: "overlapping steps",
			load: `load 10s
			    http_requests 1 5 10 20 _ 40`,
			query: "xincrease(http_requests[20s])",

			startTime: time.Unix(0, 0),
			endTime:   time.Unix(60, 0),
			step:      10 * time.Second,

			expected: promql.Matrix{
				promql.Series{
					Metric: labels.New(),
					Floats: []promql.FPoint{
						{T: 00_000, F: 1},
						{T: 10_000, F: 4},
						{T: 20_000, F: 9},
						{T: 30_000, F: 15},
						{T: 40_000, F: 10},
						{T: 50_000, F: 20},
						{T: 60_000, F: 20},
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			storage := promqltest.LoadedStorage(t, tc.load)
			defer storage.Close()

			ctx := context.Background()
			newEngine := engine.New(engine.Opts{
				EngineOpts:        opts,
				LogicalOptimizers: logicalplan.AllOptimizers,
				EnableXFunctions:  true,
			})
			query, err := newEngine.NewRangeQuery(ctx, storage, nil, tc.query, tc.startTime, tc.endTime, tc.step)
			testutil.Ok(t, err)
			defer query.Close()

			engineResult := query.Exec(ctx)
			testutil.Ok(t, engineResult.Err)

			gotMatrix, err := engineResult.Matrix()
			require.NoError(t, err)

			for i := range tc.expected {
				testutil.WithGoCmp(comparer).Equals(t, tc.expected[i].Floats, gotMatrix[i].Floats, queryExplanation(query))
			}
		})
	}
}

func TestXFunctionsWithNativeHistograms(t *testing.T) {
	defaultQueryTime := time.Unix(50, 0)

	expr := "sum(xincrease(native_histogram_series[50s]))"

	// Negative offset and at modifier are enabled by default
	// since Prometheus v2.33.0, so we also enable them.
	opts := promql.EngineOpts{
		Timeout:              1 * time.Hour,
		MaxSamples:           1e10,
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	}

	lStorage := teststorage.New(t)
	defer lStorage.Close()

	app := lStorage.Appender(context.TODO())
	testutil.Ok(t, generateFloatHistogramSeries(app, 3000, false))
	testutil.Ok(t, app.Commit())

	optimizers := logicalplan.AllOptimizers

	ctx := context.Background()
	newEngine := engine.New(engine.Opts{
		EngineOpts:        opts,
		LogicalOptimizers: optimizers,
		EnableXFunctions:  true,
	})
	query, err := newEngine.NewInstantQuery(ctx, lStorage, nil, expr, defaultQueryTime)
	testutil.Ok(t, err)
	defer query.Close()

	engineResult := query.Exec(ctx)
	require.Error(t, engineResult.Err)
}

func TestXFunctions(t *testing.T) {
	defaultQueryTime := time.Unix(50, 0)
	// Negative offset and at modifier are enabled by default
	// since Prometheus v2.33.0, so we also enable them.
	opts := promql.EngineOpts{
		Timeout:              1 * time.Hour,
		MaxSamples:           1e10,
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	}

	defaultLoad := `load 5s
	http_requests{path="/foo"}	0+10x10
	http_requests{path="/bar"}	0+10x5 0+10x4`

	xDeltaLoad := `load 5m
	http_requests{path="/foo"}	0 50 300 150 200
	http_requests{path="/bar"}	200 150 300 50 0`

	cases := []struct {
		name       string
		load       string
		query      string
		queryTime  time.Time
		expected   []promql.Sample
		rangeQuery bool
		startTime  time.Time
		endTime    time.Time
	}{
		// Tests for xIncrease
		{
			name:  "eval instant at 50s xincrease, with 50s lookback",
			load:  defaultLoad,
			query: "xincrease(http_requests[50s])",
			expected: []promql.Sample{
				createSample(defaultQueryTime.UnixMilli(), 100, labels.FromStrings("path", "/foo")),
				createSample(defaultQueryTime.UnixMilli(), 90, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:  "eval instant at 50s xincrease, with 5s lookback",
			load:  defaultLoad,
			query: "xincrease(http_requests[5s])",
			expected: []promql.Sample{
				createSample(defaultQueryTime.UnixMilli(), 10, labels.FromStrings("path", "/foo")),
				createSample(defaultQueryTime.UnixMilli(), 10, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:  "eval instant at 50s xincrease, with 10s lookback",
			load:  defaultLoad,
			query: "xincrease(http_requests[10s])",
			expected: []promql.Sample{
				createSample(defaultQueryTime.UnixMilli(), 20, labels.FromStrings("path", "/foo")),
				createSample(defaultQueryTime.UnixMilli(), 20, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:  "eval instant at 50s xincrease, with 3s lookback",
			load:  defaultLoad,
			query: "xincrease(http_requests[3s])",
			expected: []promql.Sample{
				createSample(defaultQueryTime.UnixMilli(), 10, labels.FromStrings("path", "/foo")),
				createSample(defaultQueryTime.UnixMilli(), 10, labels.FromStrings("path", "/bar")),
			},
		},
		// Additional tests
		{
			name:      "eval instant at 17s xincrease, with 5s lookback",
			load:      defaultLoad,
			query:     "xincrease(http_requests[5s])",
			queryTime: time.Unix(17, 0),
			expected: []promql.Sample{
				createSample(17000, 10, labels.FromStrings("path", "/foo")),
				createSample(17000, 10, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:      "eval instant at 17s xincrease, with 10s lookback",
			load:      defaultLoad,
			query:     "xincrease(http_requests[10s])",
			queryTime: time.Unix(17, 0),
			expected: []promql.Sample{
				createSample(17000, 20, labels.FromStrings("path", "/foo")),
				createSample(17000, 20, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:  "eval instant at 50s xrate, with 50s lookback",
			load:  defaultLoad,
			query: "xrate(http_requests[50s])",
			expected: []promql.Sample{
				createSample(defaultQueryTime.UnixMilli(), 2, labels.FromStrings("path", "/foo")),
				createSample(defaultQueryTime.UnixMilli(), 1.8, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:  "eval instant at 50s xrate, with 100s lookback",
			load:  defaultLoad,
			query: "xrate(http_requests[100s])",
			expected: []promql.Sample{
				createSample(defaultQueryTime.UnixMilli(), 1, labels.FromStrings("path", "/foo")),
				createSample(defaultQueryTime.UnixMilli(), 0.9, labels.FromStrings("path", "/bar")),
			},
		},
		// Test zero series injection.
		{
			name: "eval instant xincrease with only one point",
			load: `load 5m
			    http_requests{path="/foo"}	stale stale stale 5`,
			query:     "xincrease(http_requests[1h15m])",
			queryTime: time.Unix(1*60*60+15*60, 0),
			expected: []promql.Sample{
				createSample(time.Unix(1*60*60+15*60, 0).UnixMilli(), 5, labels.FromStrings("path", "/foo")),
			},
		},
		{
			name:  "eval instant at 50s xrate, with 5s lookback",
			load:  defaultLoad,
			query: "xrate(http_requests[5s])",
			expected: []promql.Sample{
				createSample(defaultQueryTime.UnixMilli(), 2, labels.FromStrings("path", "/foo")),
				createSample(defaultQueryTime.UnixMilli(), 2, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:  "eval instant at 50s xrate, with 3s lookback",
			load:  defaultLoad,
			query: "xrate(http_requests[3s])",
			expected: []promql.Sample{
				createSample(defaultQueryTime.UnixMilli(), 2, labels.FromStrings("path", "/foo")),
				createSample(defaultQueryTime.UnixMilli(), 2, labels.FromStrings("path", "/bar")),
			},
		},
		// # Test for increase()/xincrease with counter reset.
		// # When the counter is reset, it always starts at 0.
		// # So the sequence 3 2 (decreasing counter = reset) is interpreted the same as 3 0 1 2.
		// # Prometheus assumes it missed the intermediate values 0 and 1.
		{
			name: "eval instant at 30m increase(http_requests[30m])",
			load: `load 5m
			    http_requests{path="/foo"}	0 1 2 3 2 3 4`,
			query:     `increase(http_requests[30m])`,
			queryTime: time.Unix(1800, 0),
			expected: []promql.Sample{
				createSample(1800000, 7, labels.FromStrings("path", "/foo")),
			},
		},
		{
			name: "eval instant at 30m xincrease(http_requests[30m])",
			load: `load 5m
			    http_requests{path="/foo"}	0 1 2 3 2 3 4`,
			query:     "xincrease(http_requests[30m])",
			queryTime: time.Unix(1800, 0),
			expected: []promql.Sample{
				createSample(1800000, 7, labels.FromStrings("path", "/foo")),
			},
		},
		// Tests for xDelta
		{
			name:      "eval instant at 20m xdelta(http_requests[20m]), with 20m lookback",
			load:      xDeltaLoad,
			query:     "xdelta(http_requests[20m])",
			queryTime: time.Unix(1200, 0),
			expected: []promql.Sample{
				createSample(1200000, 200, labels.FromStrings("path", "/foo")),
				createSample(1200000, -200, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:      "eval instant at 20m xdelta(http_requests[19m]), with 19m lookback",
			load:      xDeltaLoad,
			query:     "xdelta(http_requests[19m])",
			queryTime: time.Unix(1200, 0),
			expected: []promql.Sample{
				createSample(1200000, 190, labels.FromStrings("path", "/foo")),
				createSample(1200000, -190, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:      "eval instant at 20m xdelta(http_requests[1m]), with 1m lookback",
			load:      xDeltaLoad,
			query:     "xdelta(http_requests[1m])",
			queryTime: time.Unix(1200, 0),
			expected: []promql.Sample{
				createSample(1200000, 10, labels.FromStrings("path", "/foo")),
				createSample(1200000, -10, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name: "eval instant at 4m xincrease(http_requests[2m1s]), with 1m lookback",
			load: `load 30s
			    http_requests	0 0 0 0 1 1 1 1`,
			query:     "xincrease(http_requests[2m1s])",
			queryTime: time.Unix(240, 0),
			expected: []promql.Sample{
				createSample(240000, 1, labels.Labels{}),
			},
		},
		{
			name: "eval instant at 4m xincrease(http_requests[2m]), with 1m lookback",
			load: `load 30s
			    http_requests	0 0 0 0 1 1 1 1`,
			query:     "xincrease(http_requests[2m])",
			queryTime: time.Unix(240, 0),
			expected: []promql.Sample{
				createSample(240000, 0, labels.Labels{}),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			storage := promqltest.LoadedStorage(t, tc.load)
			defer storage.Close()

			queryTime := defaultQueryTime
			if tc.queryTime != (time.Time{}) {
				queryTime = tc.queryTime
			}

			optimizers := logicalplan.AllOptimizers

			ctx := context.Background()
			newEngine := engine.New(engine.Opts{
				EngineOpts:        opts,
				LogicalOptimizers: optimizers,
				EnableXFunctions:  true,
			})
			query, err := newEngine.NewInstantQuery(ctx, storage, nil, tc.query, queryTime)
			testutil.Ok(t, err)
			defer query.Close()

			engineResult := query.Exec(ctx)
			testutil.Ok(t, engineResult.Err)
			expectedResult := createVectorResult(tc.expected)

			testutil.WithGoCmp(comparer).Equals(t, expectedResult, engineResult, queryExplanation(query))
		})
	}
}

func TestXFunctionsWhenDisabled(t *testing.T) {
	var (
		query = "xincrease(http_requests[50s])"
		start = time.Unix(0, 0)
		end   = time.Unix(100, 0)
		step  = time.Second * 10
	)
	ng := engine.New(engine.Opts{})
	_, err := ng.NewRangeQuery(context.Background(), nil, nil, query, start, end, step)
	testutil.NotOk(t, err)
	testutil.Equals(t, `1:1: parse error: unknown function with name "xincrease"`, err.Error())

	_, err = ng.NewInstantQuery(context.Background(), nil, nil, query, start)
	testutil.NotOk(t, err)
	testutil.Equals(t, `1:1: parse error: unknown function with name "xincrease"`, err.Error())
}

func TestRateVsXRate(t *testing.T) {
	defaultQueryTime := time.Unix(25, 0)
	// Negative offset and at modifier are enabled by default
	// since Prometheus v2.33.0, so we also enable them.
	opts := promql.EngineOpts{
		Timeout:              1 * time.Hour,
		MaxSamples:           1e10,
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	}

	defaultLoad := `load 5s
	http_requests{path="/foo"}  1 1 1 2 2 2 2 2 3 3 3
	http_requests{path="/bar"}  1 2 3 4 5 6 7 8 9 10 11`

	cases := []struct {
		name       string
		load       string
		query      string
		queryTime  time.Time
		expected   promql.Vector
		rangeQuery bool
		startTime  time.Time
		endTime    time.Time
	}{
		// ### Timeseries starts insice range, (presumably) goes on after range end. ###
		// 1. Reference eval
		{
			name:      "eval instant at 25s rate, with 50s lookback",
			query:     `rate(http_requests[50s])`,
			queryTime: time.Unix(25, 0),
			expected: []promql.Sample{
				createSample(defaultQueryTime.UnixMilli(), 0.022, labels.FromStrings("path", "/foo")),
				createSample(defaultQueryTime.UnixMilli(), 0.11000000000000001, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:      "eval instant at 25s xrate, with 50s lookback",
			query:     "xrate(http_requests[50s])",
			queryTime: time.Unix(25, 0),
			expected: []promql.Sample{
				createSample(defaultQueryTime.UnixMilli(), 0.02, labels.FromStrings("path", "/foo")),
				createSample(defaultQueryTime.UnixMilli(), 0.1, labels.FromStrings("path", "/bar")),
			},
		},
		// 2. Eval 1 second earlier compared to (1).
		// * path="/foo" rate should be same or fractionally higher ("shorter" sample, same actual increase);
		// * path="/bar" rate should be same or fractionally lower (80% the increase, 80/96% range covered by sample).
		// XXX Seeing ~20% jump for path="/foo"
		{
			name:      "eval instant at 24s rate(http_requests[50s]), with 50s lookback",
			query:     `rate(http_requests[50s])`,
			queryTime: time.Unix(24, 0),
			expected: []promql.Sample{
				createSample(24000, 0.0265, labels.FromStrings("path", "/foo")),
				createSample(24000, 0.106, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:      "eval instant at 24s xrate(http_requests[50s]), with 50s lookback",
			query:     "xrate(http_requests[50s])",
			queryTime: time.Unix(24, 0),
			expected: []promql.Sample{
				createSample(24000, 0.02, labels.FromStrings("path", "/foo")),
				createSample(24000, 0.08, labels.FromStrings("path", "/bar")),
			},
		},
		// 3. Eval 1 second later compared to (1)
		// * path="/foo" rate should be same or fractionally lower ("longer" sample, same actual increase).
		// * path="/bar" rate should be same or fractionally lower ("longer" sample, same actual increase).
		// XXX Higher instead of lower for both.
		{
			name:      "eval instant at 26s rate(http_requests[50s]), with 50s lookback",
			query:     `rate(http_requests[50s])`,
			queryTime: time.Unix(26, 0),
			expected: []promql.Sample{
				createSample(26000, 0.022799999999999997, labels.FromStrings("path", "/foo")),
				createSample(26000, 0.11399999999999999, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:      "eval instant at 26s xrate(http_requests[50s]), with 50s lookback",
			query:     "xrate(http_requests[50s])",
			queryTime: time.Unix(26, 0),
			expected: []promql.Sample{
				createSample(26000, 0.02, labels.FromStrings("path", "/foo")),
				createSample(26000, 0.1, labels.FromStrings("path", "/bar")),
			},
		},
		// ### Timeseries starts before range, ends within range. ###
		// 4. Reference eval
		{
			name:      "eval instant at 75s rate(http_requests[50s]), with 50s lookback",
			query:     `rate(http_requests[50s])`,
			queryTime: time.Unix(75, 0),
			expected: []promql.Sample{
				createSample(75000, 0.0275, labels.FromStrings("path", "/foo")),
				createSample(75000, 0.11, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:      "eval instant at 75s xrate(http_requests[51s]), with 50s lookback",
			query:     "xrate(http_requests[50s])",
			queryTime: time.Unix(75, 0),
			expected: []promql.Sample{
				createSample(75000, 0.02, labels.FromStrings("path", "/foo")),
				createSample(75000, 0.1, labels.FromStrings("path", "/bar")),
			},
		},
		// 5. Eval 1s earlier compared to (4)
		// * path="/foo" rate should be same or fractionally lower ("longer" sample, same actual increase).
		// * path="/bar" rate should be same or fractionally lower ("longer" sample, same actual increase).
		// # XXX Higher instead of lower for both.
		{
			name:      "eval instant at 74s rate(http_requests[50s]), with 50s lookback",
			query:     `rate(http_requests[50s])`,
			queryTime: time.Unix(74, 0),
			expected: []promql.Sample{
				createSample(74000, 0.02279999999, labels.FromStrings("path", "/foo")),
				createSample(74000, 0.11399999999, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:      "eval instant at 74s xrate(http_requests[50s]), with 50s lookback",
			query:     "xrate(http_requests[50s])",
			queryTime: time.Unix(74, 0),
			expected: []promql.Sample{
				createSample(74000, 0.02, labels.FromStrings("path", "/foo")),
				createSample(74000, 0.12, labels.FromStrings("path", "/bar")),
			},
		},
		// 6. Eval 1s later compared to (4). Rate/increase (should be) fractionally smaller.
		// * path="/foo" rate should be same or fractionally higher ("shorter" sample, same actual increase)
		// * path="/bar" rate should be same or fractionally lower (80% the increase, 80/96% range covered by sample).
		// XXX Seeing ~20% jump for path="/foo", decrease instead of increase for path="/bar".
		{
			name:      "eval instant at 76s rate(http_requests[50s]), with 50s lookback",
			query:     `rate(http_requests[50s])`,
			queryTime: time.Unix(76, 0),
			expected: []promql.Sample{
				createSample(76000, 0.0265, labels.FromStrings("path", "/foo")),
				createSample(76000, 0.106, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:      "eval instant at 76s xrate(http_requests[50s]), with 50s lookback",
			query:     "xrate(http_requests[50s])",
			queryTime: time.Unix(76, 0),
			expected: []promql.Sample{
				createSample(76000, 0.02, labels.FromStrings("path", "/foo")),
				createSample(76000, 0.1, labels.FromStrings("path", "/bar")),
			},
		},
		// Evaluation of 10 second rate every 10 seconds
		{
			name:      "eval instant at 9s rate(http_requests[10s]), with 10s lookback",
			query:     `rate(http_requests[10s])`,
			queryTime: time.Unix(9, 0),
			expected: []promql.Sample{
				createSample(9000, 0, labels.FromStrings("path", "/foo")),
				createSample(9000, 0.2, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:      "eval instant at 19s rate(http_requests[10s]), with 10s lookback",
			query:     `rate(http_requests[10s])`,
			queryTime: time.Unix(19, 0),
			expected: []promql.Sample{
				createSample(19000, 0.2, labels.FromStrings("path", "/foo")),
				createSample(19000, 0.2, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:      "eval instant at 29s rate(http_requests[10s]), with 10s lookback",
			query:     `rate(http_requests[10s])`,
			queryTime: time.Unix(29, 0),
			expected: []promql.Sample{
				createSample(29000, 0, labels.FromStrings("path", "/foo")),
				createSample(29000, 0.2, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:      "eval instant at 39s rate(http_requests[10s]), with 10s lookback",
			query:     `rate(http_requests[10s])`,
			queryTime: time.Unix(39, 0),
			expected: []promql.Sample{
				createSample(39000, 0, labels.FromStrings("path", "/foo")),
				createSample(39000, 0.2, labels.FromStrings("path", "/bar")),
			},
		},
		// XXX Missed an increase in path="/foo" between timestamps 35 and 40 (both in this eval and the one before).
		{
			name:      "eval instant at 49s rate(http_requests[10s]), with 10s lookback",
			query:     `rate(http_requests[10s])`,
			queryTime: time.Unix(49, 0),
			expected: []promql.Sample{
				createSample(49000, 0, labels.FromStrings("path", "/foo")),
				createSample(49000, 0.2, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:      "eval instant at 9s xrate(http_requests[50s]), with 10s lookback",
			query:     "xrate(http_requests[10s])",
			queryTime: time.Unix(9, 0),
			expected: []promql.Sample{
				createSample(9000, 0, labels.FromStrings("path", "/foo")),
				createSample(9000, 0.1, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:      "eval instant at 19s xrate(http_requests[50s]), with 10s lookback",
			query:     "xrate(http_requests[10s])",
			queryTime: time.Unix(19, 0),
			expected: []promql.Sample{
				createSample(19000, 0.1, labels.FromStrings("path", "/foo")),
				createSample(19000, 0.2, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:      "eval instant at 29s xrate(http_requests[50s]), with 10s lookback",
			query:     "xrate(http_requests[10s])",
			queryTime: time.Unix(29, 0),
			expected: []promql.Sample{
				createSample(29000, 0, labels.FromStrings("path", "/foo")),
				createSample(29000, 0.2, labels.FromStrings("path", "/bar")),
			},
		},
		{
			name:      "eval instant at 39s xrate(http_requests[50s]), with 10s lookback",
			query:     "xrate(http_requests[10s])",
			queryTime: time.Unix(39, 0),
			expected: []promql.Sample{
				createSample(39000, 0, labels.FromStrings("path", "/foo")),
				createSample(39000, 0.2, labels.FromStrings("path", "/bar")),
			},
		},
		// Sees the increase in path="/foo" between timestamps 35 and 40.
		{
			name:      "eval instant at 49s xrate(http_requests[50s]), with 10s lookback",
			query:     "xrate(http_requests[10s])",
			queryTime: time.Unix(49, 0),
			expected: []promql.Sample{
				createSample(49000, 0.1, labels.FromStrings("path", "/foo")),
				createSample(49000, 0.2, labels.FromStrings("path", "/bar")),
			},
		},
		// xincrease injects a zero if there is only one sample in the given timerange.
		{
			name:      "eval instant at 1s xincrease(http_requests[50s]), with 5s lookback",
			query:     "xincrease(http_requests[5s])",
			queryTime: time.Unix(1, 0),
			expected: []promql.Sample{
				createSample(1000, 1, labels.FromStrings("path", "/foo")),
				createSample(1000, 1, labels.FromStrings("path", "/bar")),
			},
		},
		// xincrease injects a zero if there is only one sample in the given timerange.
		{
			name:      "eval instant at 1s xincrease(http_requests[50s]), with 5s lookback",
			query:     "xincrease(http_requests[5s])",
			queryTime: time.Unix(1, 0),
			expected: []promql.Sample{
				createSample(1000, 1, labels.FromStrings("path", "/foo")),
				createSample(1000, 1, labels.FromStrings("path", "/bar")),
			},
		},
		// xincrease does not inject anything at the end of the given timerange if there are two or more samples.
		{
			name:      "eval instant at 55s xincrease(http_requests[10s]), with 10s lookback",
			query:     "xincrease(http_requests[10s])",
			queryTime: time.Unix(55, 0),
			expected: []promql.Sample{
				createSample(55000, 0, labels.FromStrings("path", "/foo")),
				createSample(55000, 1, labels.FromStrings("path", "/bar")),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			load := defaultLoad
			if tc.load != "" {
				load = tc.load
			}

			storage := promqltest.LoadedStorage(t, load)
			defer storage.Close()

			queryTime := defaultQueryTime
			if tc.queryTime != (time.Time{}) {
				queryTime = tc.queryTime
			}

			optimizers := logicalplan.AllOptimizers

			newEngine := engine.New(engine.Opts{
				EngineOpts:        opts,
				LogicalOptimizers: optimizers,
				EnableXFunctions:  true,
			})
			query, err := newEngine.NewInstantQuery(context.Background(), storage, nil, tc.query, queryTime)
			testutil.Ok(t, err)
			defer query.Close()

			engineResult := query.Exec(context.Background())
			expectedResult := createVectorResult(tc.expected)

			testutil.WithGoCmp(comparer).Equals(t, expectedResult, engineResult, queryExplanation(query))
		})
	}
}

func createSample(t int64, v float64, metric labels.Labels) promql.Sample {
	return promql.Sample{
		T:      t,
		F:      v,
		H:      nil,
		Metric: metric,
	}
}

func createVectorResult(vector promql.Vector) *promql.Result {
	return &promql.Result{
		Err:      nil,
		Value:    vector,
		Warnings: nil,
	}
}

func TestInstantQuery(t *testing.T) {
	t.Parallel()

	defaultQueryTime := time.Unix(50, 0)

	// Load test cases from testdata/instant.test
	cases, err := parseTestFile(filepath.Join("testdata", "instant.test"))
	if err != nil {
		t.Fatalf("failed to load instant test file: %v", err)
	}

	// Convert to instant query test cases
	instantCases := make([]struct {
		load      string
		name      string
		query     string
		queryTime time.Time
	}, len(cases))

	for i, tc := range cases {
		instantCases[i] = struct {
			load      string
			name      string
			query     string
			queryTime time.Time
		}{
			load:      tc.load,
			name:      tc.name,
			query:     tc.query,
			queryTime: tc.start, // For instant queries, start == queryTime
		}
	}

	disableOptimizerOpts := []bool{true, false}
	lookbackDeltas := []time.Duration{0, 30 * time.Second, time.Minute, 5 * time.Minute, 10 * time.Minute}
	for _, tc := range instantCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testStorage := promqltest.LoadedStorage(t, tc.load)
			defer testStorage.Close()
			for _, disableOptimizers := range disableOptimizerOpts {
				t.Run(fmt.Sprintf("disableOptimizers=%t", disableOptimizers), func(t *testing.T) {
					for _, lookbackDelta := range lookbackDeltas {
						// Negative offset and at modifier are enabled by default
						// since Prometheus v2.33.0, so we also enable them.
						testOpts := promql.EngineOpts{
							Timeout:                  1 * time.Hour,
							MaxSamples:               1e10,
							EnableNegativeOffset:     true,
							EnableAtModifier:         true,
							NoStepSubqueryIntervalFn: func(rangeMillis int64) int64 { return 30 * time.Second.Milliseconds() },
							LookbackDelta:            lookbackDelta,
						}

						var queryTime time.Time = defaultQueryTime
						if tc.queryTime != (time.Time{}) {
							queryTime = tc.queryTime
						}

						optimizers := logicalplan.AllOptimizers
						if disableOptimizers {
							optimizers = logicalplan.NoOptimizers
						}
						newEngine := engine.New(engine.Opts{
							EngineOpts:        testOpts,
							LogicalOptimizers: optimizers,
						})

						ctx := context.Background()
						q1, err := newEngine.NewInstantQuery(ctx, testStorage, nil, tc.query, queryTime)
						testutil.Ok(t, err)
						defer q1.Close()

						newResult := q1.Exec(ctx)

						oldEngine := promql.NewEngine(testOpts)
						q2, err := oldEngine.NewInstantQuery(ctx, testStorage, nil, tc.query, queryTime)
						testutil.Ok(t, err)
						defer q2.Close()

						oldResult := q2.Exec(ctx)
						testutil.WithGoCmp(comparer).Equals(t, oldResult, newResult, queryExplanation(q1))
					}
				})
			}
		})
	}
}

func TestQueryCancellation(t *testing.T) {
	twelveHours := int64(12 * time.Hour.Seconds())

	start := time.Unix(0, 0)
	end := time.Unix(twelveHours, 0)
	step := time.Second * 30
	query := `sum(rate(http_requests_total{pod="nginx-1"}[10s]))`

	querier := &storage.MockQueryable{
		MockQuerier: &storage.MockQuerier{
			SelectMockFunction: func(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
				return newTestSeriesSet(&slowSeries{})
			},
		},
	}

	ctx := context.Background()
	newEngine := engine.New(engine.Opts{EngineOpts: promql.EngineOpts{Timeout: 1 * time.Hour}})
	q1, err := newEngine.NewRangeQuery(ctx, querier, nil, query, start, end, step)
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		<-time.After(1000 * time.Millisecond)
		cancel()
	}()

	newResult := q1.Exec(ctx)
	testutil.Equals(t, context.Canceled, newResult.Err)
}

func TestQueryConcurrency(t *testing.T) {
	const storageDelay = 200 * time.Millisecond
	queryable := &storage.MockQueryable{
		MockQuerier: &storage.MockQuerier{
			SelectMockFunction: func(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
				return newSlowSeriesSet(storageDelay)
			},
		},
	}

	var (
		ctx          = context.Background()
		logger       = promslog.New(&promslog.Config{Writer: os.Stdout})
		concurrency  = 2
		maxQueries   = 4
		responseChan = make(chan struct{}, maxQueries)
	)
	newEngine := engine.New(engine.Opts{
		EngineOpts: promql.EngineOpts{
			Timeout:            1 * time.Hour,
			MaxSamples:         math.MaxInt64,
			ActiveQueryTracker: promql.NewActiveQueryTracker(t.TempDir(), concurrency, logger),
		}},
	)
	for range maxQueries {
		go func() {
			qry, err := newEngine.NewRangeQuery(ctx, queryable, nil, `count(metric)`, time.Unix(0, 0), time.Unix(300, 0), time.Second*30)
			testutil.Ok(t, err)

			resp := qry.Exec(ctx)
			testutil.Ok(t, resp.Err)

			responseChan <- struct{}{}
		}()
	}

	var (
		i           = 0
		gracePeriod = storageDelay + 10*time.Millisecond
	)
	for i < concurrency {
		select {
		case <-time.After(gracePeriod):
			t.Errorf("expected query to complete within %f seconds", gracePeriod.Seconds())
		case <-responseChan:
		}
		i++
	}
	select {
	case <-responseChan:
		t.Error("Expected to block on a query but did not")
	case <-time.After(10 * time.Millisecond):
		break
	}
	for i < maxQueries {
		select {
		case <-time.After(gracePeriod):
			t.Errorf("expected query to complete within %f seconds", gracePeriod.Seconds())
		case <-responseChan:
		}
		i++
	}
}

func TestQueryTimeout(t *testing.T) {
	end := time.Unix(120, 0)
	query := `http_requests_total{pod="nginx-1"}`
	load := `load 30s
				http_requests_total{pod="nginx-1"} 1+1x1
				http_requests_total{pod="nginx-2"} 1+2x1`

	opts := promql.EngineOpts{
		Timeout:    1 * time.Microsecond,
		MaxSamples: math.MaxInt64,
	}

	storage := promqltest.LoadedStorage(t, load)
	defer storage.Close()

	newEngine := engine.New(engine.Opts{EngineOpts: opts})

	q, err := newEngine.NewInstantQuery(context.Background(), storage, nil, query, end)
	testutil.Ok(t, err)

	res := q.Exec(context.Background())
	testutil.NotOk(t, res.Err, "expected timeout error but got none")
	testutil.Equals(t, context.DeadlineExceeded, res.Err)
}

type hintRecordingQuerier struct {
	storage.Querier
	mux   sync.Mutex
	hints []*storage.SelectHints
}

func (h *hintRecordingQuerier) Close() error { return nil }

func (h *hintRecordingQuerier) Select(_ context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	h.mux.Lock()
	defer h.mux.Unlock()
	h.hints = append(h.hints, hints)
	return storage.EmptySeriesSet()
}

func TestSelectHintsSetCorrectly(t *testing.T) {
	for _, tc := range []struct {
		query string

		// All times are in milliseconds.
		start int64
		end   int64

		// TODO(bwplotka): Add support for better hints when subquerying.
		expected []*storage.SelectHints
	}{
		{
			query: `foo`, start: 10000,
			expected: []*storage.SelectHints{
				{Start: 5000 + 1, End: 10000},
			},
		}, {
			query: `foo @ 15.000`, start: 10000,
			expected: []*storage.SelectHints{
				{Start: 10000 + 1, End: 15000},
			},
		}, {
			query: `foo @ 1.000`, start: 10000,
			expected: []*storage.SelectHints{
				{Start: -4000 + 1, End: 1000},
			},
		}, {
			query: `rate(foo[2m])`, start: 200000,
			expected: []*storage.SelectHints{
				{Start: 80000 + 1, End: 200000, Range: 120000, Func: "rate"},
			},
		}, {
			query: `rate(foo[2m] @ 180.000)`, start: 200000,
			expected: []*storage.SelectHints{
				{Start: 60000 + 1, End: 180000, Range: 120000, Func: "rate"},
			},
		}, {
			query: `rate(foo[2m] @ 300.000)`, start: 200000,
			expected: []*storage.SelectHints{
				{Start: 180000 + 1, End: 300000, Range: 120000, Func: "rate"},
			},
		}, {
			query: `rate(foo[2m] @ 60.000)`, start: 200000,
			expected: []*storage.SelectHints{
				{Start: -60000 + 1, End: 60000, Range: 120000, Func: "rate"},
			},
		}, {
			query: `rate(foo[2m] offset 2m)`, start: 300000,
			expected: []*storage.SelectHints{
				{Start: 60000 + 1, End: 180000, Range: 120000, Func: "rate"},
			},
		}, {
			query: `rate(foo[2m] @ 200.000 offset 2m)`, start: 300000,
			expected: []*storage.SelectHints{
				{Start: -40000 + 1, End: 80000, Range: 120000, Func: "rate"},
			},
		}, {
			query: `rate(foo[2m:1s])`, start: 300000,
			expected: []*storage.SelectHints{
				{Start: 175000 + 1, End: 300000, Step: 1000, Func: "rate"},
			},
		}, {
			query: `count_over_time(foo[2m:1s])`, start: 300000,
			expected: []*storage.SelectHints{
				{Start: 175000 + 1, End: 300000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: `count_over_time(foo[2m:1s] @ 300.000)`, start: 200000,
			expected: []*storage.SelectHints{
				{Start: 175000 + 1, End: 300000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: `count_over_time(foo[2m:1s] @ 200.000)`, start: 200000,
			expected: []*storage.SelectHints{
				{Start: 75000 + 1, End: 200000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: `count_over_time(foo[2m:1s] @ 100.000)`, start: 200000,
			expected: []*storage.SelectHints{
				{Start: -25000 + 1, End: 100000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: `count_over_time(foo[2m:1s] offset 10s)`, start: 300000,
			expected: []*storage.SelectHints{
				{Start: 165000 + 1, End: 290000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: `count_over_time((foo offset 10s)[2m:1s] offset 10s)`, start: 300000,
			expected: []*storage.SelectHints{
				{Start: 155000 + 1, End: 280000, Func: "count_over_time", Step: 1000},
			},
		}, {
			// When the @ is on the vector selector, the enclosing subquery parameters
			// don't affect the hint ranges.
			query: `count_over_time((foo @ 200.000 offset 10s)[2m:1s] offset 10s)`, start: 300000,
			expected: []*storage.SelectHints{
				{Start: 185000 + 1, End: 190000, Func: "count_over_time", Step: 1000},
			},
		}, {
			// When the @ is on the vector selector, the enclosing subquery parameters
			// don't affect the hint ranges.
			query: `count_over_time((foo @ 200.000 offset 10s)[2m:1s] @ 100.000 offset 10s)`, start: 300000,
			expected: []*storage.SelectHints{
				{Start: 185000 + 1, End: 190000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: `count_over_time((foo offset 10s)[2m:1s] @ 100.000 offset 10s)`, start: 300000,
			expected: []*storage.SelectHints{
				{Start: -45000 + 1, End: 80000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: `foo`, start: 10000, end: 20000,
			expected: []*storage.SelectHints{
				{Start: 5000 + 1, End: 20000, Step: 1000},
			},
		}, {
			query: `foo @ 15.000`, start: 10000, end: 20000,
			expected: []*storage.SelectHints{
				{Start: 10000 + 1, End: 15000, Step: 1000},
			},
		}, {
			query: `foo @ 1.000`, start: 10000, end: 20000,
			expected: []*storage.SelectHints{
				{Start: -4000 + 1, End: 1000, Step: 1000},
			},
		}, {
			query: `rate(foo[2m] @ 180.000)`, start: 200000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 60000 + 1, End: 180000, Range: 120000, Func: "rate", Step: 1000},
			},
		}, {
			query: `rate(foo[2m] @ 300.000)`, start: 200000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 180000 + 1, End: 300000, Range: 120000, Func: "rate", Step: 1000},
			},
		}, {
			query: `rate(foo[2m] @ 60.000)`, start: 200000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: -60000 + 1, End: 60000, Range: 120000, Func: "rate", Step: 1000},
			},
		}, {
			query: `rate(foo[2m])`, start: 200000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 80000 + 1, End: 500000, Range: 120000, Func: "rate", Step: 1000},
			},
		}, {
			query: `rate(foo[2m] offset 2m)`, start: 300000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 60000 + 1, End: 380000, Range: 120000, Func: "rate", Step: 1000},
			},
		}, {
			query: `rate(foo[2m:1s])`, start: 300000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 175000 + 1, End: 500000, Func: "rate", Step: 1000},
			},
		}, {
			query: `count_over_time(foo[2m:1s])`, start: 300000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 175000 + 1, End: 500000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: `count_over_time(foo[2m:1s] offset 10s)`, start: 300000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 165000 + 1, End: 490000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: `count_over_time(foo[2m:1s] @ 300.000)`, start: 200000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 175000 + 1, End: 300000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: `count_over_time(foo[2m:1s] @ 200.000)`, start: 200000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 75000 + 1, End: 200000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: `count_over_time(foo[2m:1s] @ 100.000)`, start: 200000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: -25000 + 1, End: 100000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: `count_over_time((foo offset 10s)[2m:1s] offset 10s)`, start: 300000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 155000 + 1, End: 480000, Func: "count_over_time", Step: 1000},
			},
		}, {
			// When the @ is on the vector selector, the enclosing subquery parameters
			// don't affect the hint ranges.
			query: `count_over_time((foo @ 200.000 offset 10s)[2m:1s] offset 10s)`, start: 300000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 185000 + 1, End: 190000, Func: "count_over_time", Step: 1000},
			},
		}, {
			// When the @ is on the vector selector, the enclosing subquery parameters
			// don't affect the hint ranges.
			query: `count_over_time((foo @ 200.000 offset 10s)[2m:1s] @ 100.000 offset 10s)`, start: 300000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 185000 + 1, End: 190000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: `count_over_time((foo offset 10s)[2m:1s] @ 100.000 offset 10s)`, start: 300000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: -45000 + 1, End: 80000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: `sum by (dim1) (foo)`, start: 10000,
			expected: []*storage.SelectHints{
				{Start: 5000 + 1, End: 10000, Func: "sum", By: true, Grouping: []string{"dim1"}},
			},
		}, {
			query: `sum without (dim1) (foo)`, start: 10000,
			expected: []*storage.SelectHints{
				{Start: 5000 + 1, End: 10000, Func: "sum", Grouping: []string{"dim1"}},
			},
		}, {
			query: `sum by (dim1) (avg_over_time(foo[1s]))`, start: 10000,
			expected: []*storage.SelectHints{
				{Start: 9000 + 1, End: 10000, Func: "avg_over_time", Range: 1000},
			},
		}, {
			query: `sum by (dim1) (max by (dim2) (foo))`, start: 10000,
			expected: []*storage.SelectHints{
				{Start: 5000 + 1, End: 10000, Func: "max", By: true, Grouping: []string{"dim2"}},
			},
		}, {
			query: `max_over_time((max by (dim1) (foo))[5s:1s])`, start: 10000,
			expected: []*storage.SelectHints{
				{Start: 0 + 1, End: 10000, Func: "max", By: true, Grouping: []string{"dim1"}, Step: 1000},
			},
		}, {
			query: "max_over_time((sum(http_requests{group=~\"p.*\"})+max(http_requests{group=~\"c.*\"}))[20s:5s])", start: 120000,
			expected: []*storage.SelectHints{
				{Start: 95000 + 1, End: 120000, Func: "sum", By: true, Step: 5000},
				{Start: 95000 + 1, End: 120000, Func: "max", By: true, Step: 5000},
			},
		}, {
			query: `foo @ 50.000 + bar @ 250.000 + baz @ 900.000`, start: 100000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 45000 + 1, End: 50000, Step: 1000},
				{Start: 245000 + 1, End: 250000, Step: 1000},
				{Start: 895000 + 1, End: 900000, Step: 1000},
			},
		}, {
			query: `foo @ 50.000 + bar + baz @ 900.000`, start: 100000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 45000 + 1, End: 50000, Step: 1000},
				{Start: 95000 + 1, End: 500000, Step: 1000},
				{Start: 895000 + 1, End: 900000, Step: 1000},
			},
		}, {
			query: `rate(foo[2s] @ 50.000) + bar @ 250.000 + baz @ 900.000`, start: 100000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 48000 + 1, End: 50000, Step: 1000, Func: "rate", Range: 2000},
				{Start: 245000 + 1, End: 250000, Step: 1000},
				{Start: 895000 + 1, End: 900000, Step: 1000},
			},
		}, {
			query: `rate(foo[2s:1s] @ 50.000) + bar + baz`, start: 100000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 43000 + 1, End: 50000, Step: 1000, Func: "rate"},
				{Start: 95000 + 1, End: 500000, Step: 1000},
				{Start: 95000 + 1, End: 500000, Step: 1000},
			},
		}, {
			query: `rate(foo[2s:1s] @ 50.000) + bar + rate(baz[2m:1s] @ 900.000 offset 2m)`, start: 100000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 43000 + 1, End: 50000, Step: 1000, Func: "rate"},
				{Start: 95000 + 1, End: 500000, Step: 1000},
				{Start: 655000 + 1, End: 780000, Step: 1000, Func: "rate"},
			},
		}, { // Hints are based on the inner most subquery timestamp.
			query: `
sum_over_time(
sum_over_time(sum_over_time(metric{job="1"}[1m40s])[1m40s:25s] @ 50.000)[3s:1s] @ 3000.000
)`, start: 100000,
			expected: []*storage.SelectHints{
				{Start: -150000 + 1, End: 50000, Range: 100000, Func: "sum_over_time", Step: 25000},
			},
		}, { // Hints are based on the inner most subquery timestamp.
			query: `
sum_over_time(
sum_over_time(sum_over_time(metric{job="1"}[1m40s])[1m40s:25s] @ 3000.000)[3s:1s] @ 50.000
)`,
			expected: []*storage.SelectHints{
				{Start: 2800000 + 1, End: 3000000, Range: 100000, Func: "sum_over_time", Step: 25000},
			},
		},
	} {
		t.Run(tc.query, func(t *testing.T) {
			t.Parallel()
			opts := promql.EngineOpts{
				Logger:           nil,
				Reg:              nil,
				MaxSamples:       10,
				Timeout:          10 * time.Second,
				LookbackDelta:    5 * time.Second,
				EnableAtModifier: true,
			}

			ng := engine.New(engine.Opts{EngineOpts: opts})
			hintsRecorder := &hintRecordingQuerier{}
			queryable := &storage.MockQueryable{MockQuerier: hintsRecorder}
			ctx := context.Background()

			var (
				query promql.Query
				err   error
			)
			if tc.end == 0 {
				query, err = ng.NewInstantQuery(ctx, queryable, nil, tc.query, timestamp.Time(tc.start))
			} else {
				query, err = ng.NewRangeQuery(ctx, queryable, nil, tc.query, timestamp.Time(tc.start), timestamp.Time(tc.end), time.Second)
			}
			testutil.Ok(t, err)

			res := query.Exec(context.Background())
			testutil.Ok(t, res.Err)

			// Selects are done in parallel so check that all hints are
			// present, but order does not matter.
			testutil.Equals(t, len(tc.expected), len(hintsRecorder.hints))
			for _, expected := range tc.expected {
				contains := false
				for _, hint := range hintsRecorder.hints {
					if reflect.DeepEqual(expected, hint) {
						contains = true
					}
				}
				testutil.Assert(t, contains, "hints did not contain contain %#v", expected)
			}
		})
	}
}

func TestQueryStats(t *testing.T) {
	cases := []struct {
		name  string
		load  string
		query string
		start time.Time
		end   time.Time
		step  time.Duration
	}{
		{
			name: "nested subquery",
			load: `load 15s
			    http_requests_total{pod="nginx-1"} 1+2x1000
			    http_requests_total{pod="nginx-2"} 1+3x10`,
			query: `sum_over_time(deriv(rate(http_requests_total[30s])[1m:30s])[2m:])`,
			start: time.Unix(0, 0),
			end:   time.Unix(3600, 0),
			step:  time.Second * 10,
		},
		{
			name: "subquery",
			load: `load 15s
			    http_requests_total{pod="nginx-1"} 1+2x200
			    http_requests_total{pod="nginx-2"} 1+3x200`,
			query: `max_over_time(sum(http_requests_total)[30s:15s])`,
			start: time.Unix(0, 0),
			end:   time.Unix(1500, 0),
			step:  time.Second * 30,
		},
		{
			name: "subquery different time range",
			load: `load 15s
			    http_requests_total{pod="nginx-1"} 1+2x200
			    http_requests_total{pod="nginx-2"} 1+3x200`,
			query: `max_over_time(sum(http_requests_total)[30s:15s])`,
			start: time.Unix(60, 0),
			end:   time.Unix(1000, 0),
			step:  time.Second * 30,
		},
		{
			name: "vector selector",
			load: `load 30s
			    http_requests_total{pod="nginx-1"} 1+1x100
			    http_requests_total{pod="nginx-2"} 1+2x100`,
			query: `http_requests_total{pod="nginx-1"}`,
			start: time.Unix(0, 0),
			end:   time.Unix(1800, 0),
			step:  time.Second * 30,
		},
		{
			name: "vector selector sparse",
			load: `load 30s
			    http_requests_total{pod="nginx-1"} 1+1x100
			    http_requests_total{pod="nginx-2"} 1+2x20`,
			query: `rate(http_requests_total{pod="nginx-2"}[10s])`,
			start: time.Unix(0, 0),
			end:   time.Unix(1800, 0),
			step:  time.Second * 30,
		},
		{
			name: "sum",
			load: `load 30s
			    http_requests_total{pod="nginx-1"} 1+1x100
			    http_requests_total{pod="nginx-2"} 1+2x100`,
			query: `sum(http_requests_total)`,
			start: time.Unix(0, 0),
			end:   time.Unix(1200, 0),
			step:  time.Second * 30,
		},
		{
			name: "sum rate",
			load: `load 30s
			    http_requests_total{pod="nginx-1"} 1+1x100
			    http_requests_total{pod="nginx-2"} 1+2x100`,
			query: `sum(rate(http_requests_total[1m]))`,
			start: time.Unix(0, 0),
			end:   time.Unix(1800, 0),
			step:  time.Second * 30,
		},
		{
			name: "sum rate large window",
			load: `load 2m
			    http_requests_total{pod="nginx-1"} 1+1x100
			    http_requests_total{pod="nginx-2"} 1+2x100`,
			query: `sum(rate(http_requests_total[1m]))`,
			start: time.Unix(0, 0),
			end:   time.Unix(1800, 0),
			step:  time.Second * 30,
		},
		{
			name: "sum rate sparse",
			load: `load 2m
			    http_requests_total{pod="nginx-1"} 1+1x5
			    http_requests_total{pod="nginx-2"} 1+2x5`,
			query: `sum(rate(http_requests_total[1m]))`,
			start: time.Unix(0, 0),
			end:   time.Unix(1800, 0),
			step:  time.Second * 30,
		},
		{
			name: "label_replace",
			load: `load 2m
			    http_requests_total{pod="nginx-1"} 1+1x5
			    http_requests_total{pod="nginx-2"} 1+2x5`,
			query: `label_replace(http_requests_total, "replace", "$1", "pod", "(.*)")`,
			start: time.Unix(1, 0),
			end:   time.Unix(1800, 0),
			step:  time.Second * 30,
		},
		{
			name: "step invariant with samples",
			load: `load 5m
			    http_requests_total{pod="nginx-1"} 1+1x5
			    http_requests_total{pod="nginx-2"} 1+2x5`,
			query: `sum without (__name__) (http_requests_total @ end())`,
			start: time.Unix(1, 0),
			end:   time.Unix(600, 0),
			step:  time.Second * 34,
		},
		{
			name: "step invariant without samples",
			load: `load 30s
			    http_requests_total{pod="nginx-1"} 1.00+1.00x15
			    http_requests_total{pod="nginx-2"}  1+2.00x21`,
			query: `pi()`,
			start: time.UnixMilli(0),
			end:   time.UnixMilli(120000),
			step:  time.Second * 30,
		},
		{
			name: "fuzz subquery without enough samples",
			load: `load 30s
			    http_requests_total{pod="nginx-1"} 1.00+1.00x15
			    http_requests_total{pod="nginx-2"}  1+2.00x21`,
			query: `rate({__name__="http_requests_total"} offset -6s[1h:1m] offset 1m29s)`,
			start: time.UnixMilli(0),
			end:   time.UnixMilli(120000),
			step:  time.Second * 30,
		},
		{
			name: "native histogram sum compact",
			load: `load 2m
			    http_request_duration_seconds{pod="nginx-1"} {{schema:0 count:3 sum:14.00 buckets:[1 2]}}+{{schema:0 count:4 buckets:[1 2 1]}}x20
			    http_request_duration_seconds{pod="nginx-2"} {{schema:0 count:2 sum:14.00 buckets:[2]}}+{{schema:0 count:6 buckets:[2 2 2]}}x20`,
			query: `--sum by (pod) ({__name__="http_request_duration_seconds"})`,
			start: time.UnixMilli(0),
			end:   time.UnixMilli(2400000),
			step:  time.Second * 30,
		},
		{
			name: "native histogram rate with counter reset and step equal to window",
			load: `load 30s
			    some_metric {{schema:0 sum:1 count:1 buckets:[1]}} {{schema:0 sum:0 count:0 buckets:[1]}} {{schema:0 sum:5 count:4 buckets:[1 2 1]}} {{schema:0 sum:1 count:1 buckets:[1]}}`,
			query: `rate(some_metric[1m])`,
			start: time.Unix(-60, 0),
			end:   time.Unix(120, 0),
			step:  time.Second * 30,
		},
		{
			name: "native histogram histogram_quantile",
			load: `load 2m
			    http_request_duration_seconds{pod="nginx-1"} {{schema:0 count:3 sum:14.00 buckets:[1 2]}}+{{schema:0 count:4 buckets:[1 2 1]}}x20
			    http_request_duration_seconds{pod="nginx-2"} {{schema:0 count:2 sum:14.00 buckets:[2]}}+{{schema:0 count:6 buckets:[2 2 2]}}x20`,
			query: `histogram_quantile(0.9, {__name__="http_request_duration_seconds"})`,
			start: time.UnixMilli(0),
			end:   time.UnixMilli(2400000),
			step:  time.Second * 30,
		},
		{
			name: "fuzz aggregation with scalar param",
			load: `load 30s
			    http_requests_total{pod="nginx-1"} -77.00+1.00x15
			    http_requests_total{pod="nginx-2"}  1+0.67x21`,
			query: `
quantile without (pod) (
  scalar({__name__="http_requests_total"} offset 2m58s),
  {__name__="http_requests_total"}
)`,
			start: time.UnixMilli(0),
			end:   time.UnixMilli(221000),
			step:  time.Second * 30,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			opts := promql.EngineOpts{
				Timeout:                  300 * time.Second,
				MaxSamples:               math.MaxInt64,
				EnablePerStepStats:       true,
				EnableAtModifier:         true,
				EnableNegativeOffset:     true,
				NoStepSubqueryIntervalFn: func(rangeMillis int64) int64 { return 30 * time.Second.Milliseconds() },
			}
			qOpts := promql.NewPrometheusQueryOpts(true, 5*time.Minute)

			storage := promqltest.LoadedStorage(t, tc.load)
			defer storage.Close()

			ctx := context.Background()

			oldEngine := promql.NewEngine(opts)
			newEngine := engine.New(engine.Opts{EnableAnalysis: true, EngineOpts: opts})

			// Instant query
			oldQ, err := oldEngine.NewInstantQuery(ctx, storage, qOpts, tc.query, tc.end)
			testutil.Ok(t, err)
			oldResult := oldQ.Exec(ctx)
			oldStats := oldQ.Stats()
			stats.NewQueryStats(oldStats)

			newQ, err := newEngine.NewInstantQuery(ctx, storage, qOpts, tc.query, tc.end)
			testutil.Ok(t, err)
			newResult := newQ.Exec(ctx)
			newStats := newQ.Stats()
			stats.NewQueryStats(newStats)

			testutil.WithGoCmp(comparer).Equals(t, oldResult, newResult)
			if oldResult.Err == nil {
				testutil.WithGoCmp(samplesComparer).Equals(t, oldStats.Samples, newStats.Samples)
			}

			// Range query
			oldQ, err = oldEngine.NewRangeQuery(ctx, storage, qOpts, tc.query, tc.start, tc.end, tc.step)
			testutil.Ok(t, err)
			oldResult = oldQ.Exec(ctx)
			oldStats = oldQ.Stats()
			stats.NewQueryStats(oldStats)

			newQ, err = newEngine.NewRangeQuery(ctx, storage, qOpts, tc.query, tc.start, tc.end, tc.step)
			testutil.Ok(t, err)
			newResult = newQ.Exec(ctx)
			newStats = newQ.Stats()
			stats.NewQueryStats(newStats)

			testutil.WithGoCmp(comparer).Equals(t, oldResult, newResult)
			if oldResult.Err == nil {
				testutil.WithGoCmp(samplesComparer).Equals(t, oldStats.Samples, newStats.Samples)
			}
		})
	}
}

func storageWithMockSeries(mockSeries ...*mockSeries) *storage.MockQueryable {
	series := make([]storage.Series, 0, len(mockSeries))
	for _, mock := range mockSeries {
		series = append(series, storage.Series(mock))
	}
	return storageWithSeries(series...)
}

func storageWithSeries(series ...storage.Series) *storage.MockQueryable {
	return &storage.MockQueryable{
		MockQuerier: &storage.MockQuerier{
			SelectMockFunction: func(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
				result := make([]storage.Series, 0)
			loopSeries:
				for _, s := range series {
					for _, m := range matchers {
						lbl := s.Labels().Get(m.Name)
						if !m.Matches(lbl) {
							continue loopSeries
						}
					}
					result = append(result, s)
				}
				return newTestSeriesSet(result...)
			},
		},
	}
}

type byTimestamps mockSeries

func (b byTimestamps) Len() int {
	return len(b.timestamps)
}

func (b byTimestamps) Less(i, j int) bool {
	return b.timestamps[i] < b.timestamps[j]
}

func (b byTimestamps) Swap(i, j int) {
	b.timestamps[i], b.timestamps[j] = b.timestamps[j], b.timestamps[i]
	b.values[i], b.values[j] = b.values[j], b.values[i]
}

type mockSeries struct {
	labels     []string
	timestamps []int64
	values     []float64
}

func newMockSeries(labels []string, timestamps []int64, values []float64) *mockSeries {
	for i := range timestamps {
		timestamps[i] = timestamps[i] * 1000
	}
	return &mockSeries{labels: labels, timestamps: timestamps, values: values}
}

func (m mockSeries) Labels() labels.Labels {
	return labels.FromStrings(m.labels...)
}

func (m mockSeries) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return &mockIterator{
		i:          -1,
		timestamps: m.timestamps,
		values:     m.values,
	}
}

type mockIterator struct {
	i          int
	timestamps []int64
	values     []float64
}

func (m *mockIterator) Next() chunkenc.ValueType {
	m.i++
	if m.i >= len(m.values) {
		return chunkenc.ValNone
	}

	return chunkenc.ValFloat
}

func (m *mockIterator) Seek(t int64) chunkenc.ValueType {
	if m.i > -1 && m.i < len(m.timestamps) {
		currentTS := m.timestamps[m.i]
		if currentTS >= t {
			return chunkenc.ValFloat
		}
	}
	for {
		next := m.Next()
		if next == chunkenc.ValNone {
			return chunkenc.ValNone
		}

		if m.AtT() >= t {
			return next
		}
	}
}

func (m *mockIterator) At() (int64, float64) {
	return m.timestamps[m.i], m.values[m.i]
}

func (m *mockIterator) AtHistogram(_ *histogram.Histogram) (int64, *histogram.Histogram) {
	return 0, nil
}

func (m *mockIterator) AtFloatHistogram(_ *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return 0, nil
}

func (m *mockIterator) AtT() int64 { return m.timestamps[m.i] }

func (m *mockIterator) Err() error { return nil }

type slowSeriesSet struct {
	empty bool
	delay time.Duration
}

func newSlowSeriesSet(delay time.Duration) *slowSeriesSet {
	return &slowSeriesSet{delay: delay}
}

func (s *slowSeriesSet) Next() bool {
	if s.empty {
		return false
	}
	s.empty = true
	<-time.After(s.delay)
	return true
}

func (s slowSeriesSet) At() storage.Series {
	return storage.MockSeries([]int64{0}, []float64{0}, nil)
}

func (s slowSeriesSet) Err() error { return nil }

func (s slowSeriesSet) Warnings() annotations.Annotations { return nil }

type testSeriesSet struct {
	i      int
	series []storage.Series
	warns  annotations.Annotations
	err    error
}

func newTestSeriesSet(series ...storage.Series) storage.SeriesSet {
	return &testSeriesSet{
		i:      -1,
		series: series,
	}
}

func newWarningsSeriesSet(warns annotations.Annotations) storage.SeriesSet {
	return &testSeriesSet{
		i:     -1,
		warns: warns,
	}
}

func (s *testSeriesSet) Next() bool                        { s.i++; return s.i < len(s.series) }
func (s *testSeriesSet) At() storage.Series                { return s.series[s.i] }
func (s *testSeriesSet) Err() error                        { return s.err }
func (s *testSeriesSet) Warnings() annotations.Annotations { return s.warns }

type slowSeries struct{}

func (d slowSeries) Labels() labels.Labels                        { return labels.FromStrings("foo", "bar") }
func (d slowSeries) Iterator(chunkenc.Iterator) chunkenc.Iterator { return &slowIterator{} }

type slowIterator struct {
	ts int64
}

func (d *slowIterator) AtHistogram(_ *histogram.Histogram) (int64, *histogram.Histogram) {
	panic("not implemented")
}

func (d *slowIterator) AtFloatHistogram(_ *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	panic("not implemented")
}

func (d *slowIterator) AtT() int64 {
	return d.ts
}

func (d *slowIterator) At() (int64, float64) {
	return d.ts, 1
}

func (d *slowIterator) Next() chunkenc.ValueType {
	<-time.After(10 * time.Millisecond)
	d.ts += 30 * 1000
	return chunkenc.ValFloat
}

func (d *slowIterator) Seek(t int64) chunkenc.ValueType {
	<-time.After(10 * time.Millisecond)
	d.ts = t
	return chunkenc.ValFloat
}
func (d *slowIterator) Err() error { return nil }

type mockRuntimeErr struct{}

func (m *mockRuntimeErr) Error() string {
	return "panic!"
}

func (m *mockRuntimeErr) RuntimeError() {
}

func TestEngineRecoversFromPanic(t *testing.T) {
	t.Parallel()

	querier := &storage.MockQueryable{
		MockQuerier: &storage.MockQuerier{
			SelectMockFunction: func(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
				panic(runtime.Error(&mockRuntimeErr{}))
			},
		},
	}
	t.Run("instant", func(t *testing.T) {
		newEngine := engine.New(engine.Opts{})
		ctx := context.Background()
		q, err := newEngine.NewInstantQuery(ctx, querier, nil, "somequery", time.Time{})
		testutil.Ok(t, err)

		r := q.Exec(ctx)
		testutil.Assert(t, r.Err.Error() == "unexpected error: panic!")
	})

	t.Run("range", func(t *testing.T) {
		newEngine := engine.New(engine.Opts{})
		ctx := context.Background()
		q, err := newEngine.NewRangeQuery(ctx, querier, nil, "somequery", time.Time{}, time.Time{}, 42)
		testutil.Ok(t, err)

		r := q.Exec(ctx)
		testutil.Assert(t, r.Err.Error() == "unexpected error: panic!")
	})
}

func TestNativeHistogramRateWithNaN(t *testing.T) {
	type HPoint struct {
		T int64
		H *histogram.FloatHistogram
	}

	testStorage := teststorage.New(t)
	defer testStorage.Close()

	app := testStorage.Appender(t.Context())
	points := []HPoint{
		{T: 5574708, H: tsdbutil.GenerateTestFloatHistogram(1)},
		{T: 5604708, H: tsdbutil.GenerateTestFloatHistogram(2)},
		{T: 5634708, H: tsdbutil.GenerateTestFloatHistogram(3)},

		{T: 6146221, H: &histogram.FloatHistogram{Sum: math.NaN()}},
		{T: 6176221, H: tsdbutil.GenerateTestFloatHistogram(1)},
		{T: 6206221, H: tsdbutil.GenerateTestFloatHistogram(1)},
		{T: 6236221, H: tsdbutil.GenerateTestFloatHistogram(1)},
	}
	for _, point := range points {
		_, err := app.AppendHistogram(0, labels.FromStrings(labels.MetricName, "test_metric"), point.T, nil, point.H)
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())

	var (
		opts = engine.Opts{
			EngineOpts: promql.EngineOpts{
				Timeout:              1 * time.Hour,
				MaxSamples:           1e16,
				EnableNegativeOffset: true,
				EnableAtModifier:     true,
			},
		}
		start = time.UnixMilli(6146221)
		end   = time.UnixMilli(6236221)

		step = 60 * time.Second
	)
	execQuery := func(ng promql.QueryEngine) *promql.Result {
		qry, err := ng.NewRangeQuery(context.TODO(), testStorage, nil, "histogram_count(rate(test_metric[10m]))", start, end, step)
		require.NoError(t, err)
		return qry.Exec(context.Background())
	}

	promResult := execQuery(promql.NewEngine(opts.EngineOpts))
	newResult := execQuery(engine.New(opts))
	testutil.WithGoCmp(comparer).Equals(t, promResult, newResult)
}

type histogramTestCase struct {
	name                   string
	query                  string
	start                  time.Time
	wantEmptyForMixedTypes bool
}

type histogramGeneratorFunc func(app storage.Appender, numSeries int, withMixedTypes bool) error

func TestNativeHistograms(t *testing.T) {
	t.Parallel()
	opts := promql.EngineOpts{
		Timeout:              1 * time.Hour,
		MaxSamples:           1e16,
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	}

	cases := []histogramTestCase{
		{
			name:  "count_over_time() with different start time",
			query: `count_over_time(native_histogram_series[1m15s])`,
			start: time.Unix(400, 0),
		},
		{
			name:  "irate()",
			query: `irate(native_histogram_series[1m])`,
		},
		{
			name:  "rate()",
			query: `rate(native_histogram_series[1m])`,
		},
		{
			name:  "increase()",
			query: `increase(native_histogram_series[1m])`,
		},
		{
			name:  "delta()",
			query: `delta(native_histogram_series[1m])`,
		},
		{
			name:                   "sum()",
			query:                  `sum(native_histogram_series)`,
			wantEmptyForMixedTypes: true,
		},
		{
			name:                   "sum by (foo)",
			query:                  `sum by (foo) (native_histogram_series)`,
			wantEmptyForMixedTypes: true,
		},
		{
			name:                   "avg()",
			query:                  `avg(native_histogram_series)`,
			wantEmptyForMixedTypes: true,
		},
		{
			name:                   "avg by (foo)",
			query:                  `avg by (foo) (native_histogram_series)`,
			wantEmptyForMixedTypes: true,
		},
		{
			name:  "count",
			query: `count(native_histogram_series)`,
		},
		{
			name:  "count by (foo)",
			query: `count by (foo) (native_histogram_series)`,
		},
		{
			name:  "max",
			query: `max(native_histogram_series)`,
		},
		{
			name:  "max by (foo)",
			query: `max by (foo) (native_histogram_series)`,
		},
		{
			name:  "min",
			query: `min(native_histogram_series)`,
		},
		{
			name:  "min by (foo)",
			query: `min by (foo) (native_histogram_series)`,
		},
		{
			name:  "absent",
			query: `absent(native_histogram_series)`,
		},
		{
			name:  "histogram_sum",
			query: `histogram_sum(native_histogram_series)`,
		},
		{
			name:  "histogram_count",
			query: `histogram_count(native_histogram_series)`,
		},
		{
			name:  "histogram_avg",
			query: `histogram_avg(native_histogram_series)`,
		},
		{
			name:  "histogram_count of histogram product",
			query: `histogram_count(native_histogram_series * native_histogram_series)`,
		},
		{
			name:  "histogram_sum / histogram_count",
			query: `histogram_sum(native_histogram_series) / histogram_count(native_histogram_series)`,
		},
		{
			name:  "histogram_sum over histogram_quantile",
			query: `histogram_sum(scalar(histogram_quantile(1, sum(native_histogram_series))) * native_histogram_series)`,
		},
		{
			name: "histogram_sum over histogram_fraction",
			query: `
histogram_sum(
  scalar(histogram_fraction(-Inf, +Inf, sum(native_histogram_series))) * native_histogram_series
)`,
		},
		{
			name:  "histogram_quantile",
			query: `histogram_quantile(0.7, native_histogram_series)`,
		},
		{
			// Test strange query with a mix of histogram functions.
			name:  "histogram_quantile(histogram_sum)",
			query: `histogram_quantile(0.7, histogram_sum(native_histogram_series))`,
		},
		{
			name:  "histogram_count * histogram aggregation",
			query: `scalar(histogram_count(sum(native_histogram_series))) * sum(native_histogram_series)`,
		},
		{
			name:  "histogram_fraction",
			query: `histogram_fraction(0, 0.2, native_histogram_series)`,
		},
		{
			name:  "histogram_stdvar",
			query: `histogram_stdvar(native_histogram_series)`,
		},
		{
			name:  "histogram_stddev",
			query: `histogram_stddev(native_histogram_series)`,
		},
		{
			name:  "lhs multiplication",
			query: `native_histogram_series * 3`,
		},
		{
			name:  "rhs multiplication",
			query: `3 * native_histogram_series`,
		},
		{
			name:  "lhs division",
			query: `native_histogram_series / 2`,
		},
		{
			name:  "subqueries",
			query: `increase(rate(native_histogram_series[2m])[2m:15s])`,
		},
		{
			name: "Binary OR",
			query: `
  native_histogram_series
or
  (histogram_quantile(0.7, native_histogram_series) or rate(native_histogram_series[2m]))`,
		},
		{
			name:  "Mixed Binary OR",
			query: `sum(native_histogram_series) or native_histogram_series`, // sum will be a single float value, float series on lhs of 'or'
		},
		{
			name: "Binary AND",
			query: `
  (rate(native_histogram_series[2m]) and histogram_quantile(0.7, native_histogram_series))
and
  native_histogram_series`,
		},
		{
			name:  "Mixed Binary AND",
			query: `native_histogram_series and count(native_histogram_series)`, // count will be a single float value, float series on 'rhs' of 'and'
		},
		{
			name:  "many-to-many join Unless",
			query: `sum without (foo) (native_histogram_series) unless native_histogram_series / 2`,
		},
		{
			name:  "Mixed many-to-many join Unless",
			query: `native_histogram_series * 3 unless avg(native_histogram_series)`,
		},
		{
			name:  "Limitk aggregation",
			query: `limitk(2, native_histogram_series)`,
		},
		{
			name:  "limitk by",
			query: `limitk(2, native_histogram_series) by (foo) and native_histogram_series`,
		},
		{
			name:  "Limit_ratio aggregation",
			query: `limit_ratio(0.4, native_histogram_series)`,
		},
		{
			name:  "limit_ratio by",
			query: `limit_ratio(0.33, native_histogram_series) by (foo) or native_histogram_series`,
		},
	}

	defer pprof.StopCPUProfile()
	t.Run("integer_histograms", func(t *testing.T) {
		t.Parallel()
		testNativeHistograms(t, cases, opts, generateNativeHistogramSeries)
	})
	t.Run("float_histograms", func(t *testing.T) {
		t.Parallel()
		testNativeHistograms(t, cases, opts, generateFloatHistogramSeries)
	})
}

func testNativeHistograms(t *testing.T, cases []histogramTestCase, opts promql.EngineOpts, generateHistograms histogramGeneratorFunc) {
	numHistograms := 10
	mixedTypesOpts := []bool{false, true}
	var (
		queryStart = time.Unix(50, 0)
		queryEnd   = time.Unix(600, 0)
		queryStep  = 30 * time.Second
	)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, withMixedTypes := range mixedTypesOpts {
				t.Run(fmt.Sprintf("mixedTypes=%t", withMixedTypes), func(t *testing.T) {
					storage := teststorage.New(t)
					defer storage.Close()

					app := storage.Appender(context.TODO())
					err := generateHistograms(app, numHistograms, withMixedTypes)
					testutil.Ok(t, err)
					testutil.Ok(t, app.Commit())

					promEngine := promql.NewEngine(opts)
					thanosEngine := engine.New(engine.Opts{
						EngineOpts:        opts,
						LogicalOptimizers: logicalplan.AllOptimizers,
					})

					t.Run("instant", func(t *testing.T) {
						ctx := context.Background()
						q1, err := thanosEngine.NewInstantQuery(ctx, storage, nil, tc.query, time.Unix(50, 0))
						testutil.Ok(t, err)
						newResult := q1.Exec(ctx)
						testutil.Ok(t, newResult.Err)

						q2, err := promEngine.NewInstantQuery(ctx, storage, nil, tc.query, time.Unix(50, 0))
						testutil.Ok(t, err)
						promResult := q2.Exec(ctx)
						testutil.Ok(t, promResult.Err)
						promVector, err := promResult.Vector()
						testutil.Ok(t, err)

						// Make sure we're not getting back empty results.
						if withMixedTypes && tc.wantEmptyForMixedTypes {
							testutil.Assert(t, len(promVector) == 0)
							testutil.Equals(t, len(promResult.Warnings), len(newResult.Warnings))
						}

						testutil.WithGoCmp(comparer).Equals(t, promResult, newResult, queryExplanation(q1))
					})

					t.Run("range", func(t *testing.T) {
						if tc.start == (time.Time{}) {
							tc.start = queryStart
						}
						ctx := context.Background()
						q1, err := thanosEngine.NewRangeQuery(ctx, storage, nil, tc.query, tc.start, queryEnd, queryStep)
						testutil.Ok(t, err)
						newResult := q1.Exec(ctx)
						testutil.Ok(t, newResult.Err)

						q2, err := promEngine.NewRangeQuery(ctx, storage, nil, tc.query, tc.start, queryEnd, queryStep)
						testutil.Ok(t, err)
						promResult := q2.Exec(ctx)
						testutil.Ok(t, promResult.Err)
						promMatrix, err := promResult.Matrix()
						testutil.Ok(t, err)

						// Make sure we're not getting back empty results.
						if withMixedTypes && tc.wantEmptyForMixedTypes {
							testutil.Assert(t, len(promMatrix) == 0)
							testutil.Equals(t, len(promResult.Warnings), len(newResult.Warnings))
							testutil.Equals(t, "PromQL warning: encountered a mix of histograms and floats for aggregation", newResult.Warnings.AsErrors()[0].Error())
						}
						testutil.WithGoCmp(comparer).Equals(t, promResult, newResult, queryExplanation(q1))
					})
				})
			}
		})
	}
}

func generateNativeHistogramSeries(app storage.Appender, numSeries int, withMixedTypes bool) error {
	commonLabels := []string{labels.MetricName, "native_histogram_series", "foo", "bar"}
	series := make([][]*histogram.Histogram, numSeries)
	for i := range series {
		series[i] = tsdbutil.GenerateTestHistograms(2000)
	}
	higherSchemaHist := &histogram.Histogram{
		Schema: 3,
		PositiveSpans: []histogram.Span{
			{Offset: -5, Length: 2}, // -5 -4
			{Offset: 2, Length: 3},  // -1 0 1
			{Offset: 2, Length: 2},  // 4 5
		},
		PositiveBuckets: []int64{1, 2, -2, 1, -1, 0, 3},
		Count:           13,
	}

	for sid, histograms := range series {
		lbls := append(commonLabels, "h", strconv.Itoa(sid))
		for i := range histograms {
			ts := time.Unix(int64(i*15), 0).UnixMilli()
			if i == 0 {
				// Inject a histogram with a higher schema.
				// Regression test for:
				// * https://github.com/thanos-io/promql-engine/pull/182
				// * https://github.com/thanos-io/promql-engine/pull/183.
				if _, err := app.AppendHistogram(0, labels.FromStrings(lbls...), ts, higherSchemaHist, nil); err != nil {
					return err
				}
			}
			if _, err := app.AppendHistogram(0, labels.FromStrings(lbls...), ts, histograms[i], nil); err != nil {
				return err
			}
			if withMixedTypes {
				if _, err := app.Append(0, labels.FromStrings(append(lbls, "classic", "1", "le", "1")...), ts, float64(i)); err != nil {
					return err
				}
				if _, err := app.Append(0, labels.FromStrings(append(lbls, "classic", "1", "le", "+Inf")...), ts, float64(i*2)); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func generateFloatHistogramSeries(app storage.Appender, numSeries int, withMixedTypes bool) error {
	lbls := []string{labels.MetricName, "native_histogram_series", "foo", "bar"}
	h1 := tsdbutil.GenerateTestFloatHistograms(numSeries)
	h2 := tsdbutil.GenerateTestFloatHistograms(numSeries)
	for i := range h1 {
		ts := time.Unix(int64(i*15), 0).UnixMilli()
		if withMixedTypes {
			if _, err := app.Append(0, labels.FromStrings(append(lbls, "le", "1")...), ts, float64(i)); err != nil {
				return err
			}
			if _, err := app.Append(0, labels.FromStrings(append(lbls, "le", "+Inf")...), ts, float64(i*2)); err != nil {
				return err
			}
		}
		if _, err := app.AppendHistogram(0, labels.FromStrings(append(lbls, "h", "1")...), ts, nil, h1[i]); err != nil {
			return err
		}
		if _, err := app.AppendHistogram(0, labels.FromStrings(append(lbls, "h", "2")...), ts, nil, h2[i]); err != nil {
			return err
		}
	}
	return nil
}

func TestMixedNativeHistogramTypes(t *testing.T) {
	t.Parallel()
	histograms := tsdbutil.GenerateTestHistograms(2)

	storage := teststorage.New(t)
	defer storage.Close()

	lbls := []string{labels.MetricName, "native_histogram_series"}

	app := storage.Appender(context.TODO())
	_, err := app.AppendHistogram(0, labels.FromStrings(lbls...), 0, nil, histograms[0].ToFloat(nil))
	testutil.Ok(t, err)
	testutil.Ok(t, app.Commit())

	app = storage.Appender(context.TODO())
	_, err = app.AppendHistogram(0, labels.FromStrings(lbls...), 30_000, histograms[1], nil)
	testutil.Ok(t, err)
	testutil.Ok(t, app.Commit())

	opts := promql.EngineOpts{
		Timeout:              1 * time.Hour,
		MaxSamples:           1e10,
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	}

	engine := engine.New(engine.Opts{
		EngineOpts:        opts,
		LogicalOptimizers: logicalplan.AllOptimizers,
	})

	ctx := context.Background()

	t.Run("vector_select", func(t *testing.T) {
		qry, err := engine.NewInstantQuery(ctx, storage, nil, "sum(native_histogram_series)", time.Unix(30, 0))
		testutil.Ok(t, err)
		res := qry.Exec(context.Background())
		testutil.Ok(t, res.Err)
		actual, err := res.Vector()
		testutil.Ok(t, err)

		testutil.Equals(t, 1, len(actual), "expected vector with 1 element")
		expected := histograms[1].ToFloat(nil)
		expected.CounterResetHint = histogram.UnknownCounterReset
		testutil.Equals(t, expected, actual[0].H)
	})

	t.Run("matrix_select", func(t *testing.T) {
		qry, err := engine.NewRangeQuery(ctx, storage, nil, "rate(native_histogram_series[1m1s])", time.Unix(0, 0), time.Unix(60, 0), 60*time.Second)
		testutil.Ok(t, err)
		res := qry.Exec(context.Background())
		testutil.Ok(t, res.Err)
		actual, err := res.Matrix()
		testutil.Ok(t, err)

		testutil.Equals(t, 1, len(actual), "expected 1 series")
		testutil.Equals(t, 1, len(actual[0].Histograms), "expected 1 point")

		diff, err := histograms[1].ToFloat(nil).Sub(histograms[0].ToFloat(nil))
		testutil.Ok(t, err)
		expected := diff.Mul(1 / float64(30))
		expected.CounterResetHint = histogram.GaugeType
		testutil.Equals(t, expected, actual[0].Histograms[0].H)
	})
}

type seriesByLabels []promql.Series

func (b seriesByLabels) Len() int           { return len(b) }
func (b seriesByLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b seriesByLabels) Less(i, j int) bool { return labels.Compare(b[i].Metric, b[j].Metric) < 0 }

type samplesByLabels []promql.Sample

func (b samplesByLabels) Len() int           { return len(b) }
func (b samplesByLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b samplesByLabels) Less(i, j int) bool { return labels.Compare(b[i].Metric, b[j].Metric) < 0 }

const epsilon = 1e-6
const fraction = 1e-10

func floatsMatch(f1, f2 []float64) bool {
	if len(f1) != len(f2) {
		return false
	}
	for i, f := range f1 {
		if !cmp.Equal(f, f2[i], cmpopts.EquateNaNs(), cmpopts.EquateApprox(fraction, epsilon)) {
			return false
		}
	}
	return true
}

// spansMatch returns true if both spans represent the same bucket layout
// after combining zero length spans with the next non-zero length span.
// Copied from: https://github.com/prometheus/prometheus/blob/3d245e31d31774f62ff18c36039315fa55fe252c/model/histogram/histogram.go#L287
func spansMatch(s1, s2 []histogram.Span) bool {
	if len(s1) == 0 && len(s2) == 0 {
		return true
	}

	s1idx, s2idx := 0, 0
	for {
		if s1idx >= len(s1) {
			return allEmptySpans(s2[s2idx:])
		}
		if s2idx >= len(s2) {
			return allEmptySpans(s1[s1idx:])
		}

		currS1, currS2 := s1[s1idx], s2[s2idx]
		s1idx++
		s2idx++
		if currS1.Length == 0 {
			// This span is zero length, so we add consecutive such spans
			// until we find a non-zero span.
			for ; s1idx < len(s1) && s1[s1idx].Length == 0; s1idx++ {
				currS1.Offset += s1[s1idx].Offset
			}
			if s1idx < len(s1) {
				currS1.Offset += s1[s1idx].Offset
				currS1.Length = s1[s1idx].Length
				s1idx++
			}
		}
		if currS2.Length == 0 {
			// This span is zero length, so we add consecutive such spans
			// until we find a non-zero span.
			for ; s2idx < len(s2) && s2[s2idx].Length == 0; s2idx++ {
				currS2.Offset += s2[s2idx].Offset
			}
			if s2idx < len(s2) {
				currS2.Offset += s2[s2idx].Offset
				currS2.Length = s2[s2idx].Length
				s2idx++
			}
		}

		if currS1.Length == 0 && currS2.Length == 0 {
			// The last spans of both set are zero length. Previous spans match.
			return true
		}

		if currS1.Offset != currS2.Offset || currS1.Length != currS2.Length {
			return false
		}
	}
}

func allEmptySpans(s []histogram.Span) bool {
	for _, ss := range s {
		if ss.Length > 0 {
			return false
		}
	}
	return true
}

var (
	// comparer should be used to compare promql results between engines.
	comparer = cmp.Comparer(func(x, y *promql.Result) bool {
		compareFloats := func(l, r float64) bool {
			return cmp.Equal(l, r, cmpopts.EquateNaNs(), cmpopts.EquateApprox(fraction, epsilon))
		}
		compareHistograms := func(l, r *histogram.FloatHistogram) bool {
			if l == nil && r == nil {
				return true
			}

			if l == nil && r != nil {
				return false
			}

			// Copied from https://github.com/prometheus/prometheus/blob/3d245e31d31774f62ff18c36039315fa55fe252c/model/histogram/float_histogram.go#L471
			// and extended to use approx comparison instead of exact match.
			if l.Schema != r.Schema || !compareFloats(l.Count, r.Count) || !compareFloats(l.Sum, r.Sum) {
				return false
			}

			if l.UsesCustomBuckets() {
				if !floatsMatch(l.CustomValues, r.CustomValues) {
					return false
				}
			}

			if l.ZeroThreshold != r.ZeroThreshold || !compareFloats(l.ZeroCount, r.ZeroCount) {
				return false
			}

			if !spansMatch(l.NegativeSpans, r.NegativeSpans) {
				return false
			}

			if !floatsMatch(l.NegativeBuckets, r.NegativeBuckets) {
				return false
			}

			if !spansMatch(l.PositiveSpans, r.PositiveSpans) {
				return false
			}

			if !floatsMatch(l.PositiveBuckets, r.PositiveBuckets) {
				return false
			}

			return true
		}
		compareAnnotations := func(l, r annotations.Annotations) bool {
			// TODO: discard promql annotations for now, once we support them we should add them back
			discardPromqlAnnotations := func(k string, _ error) bool {
				hasInfoPrefix := strings.HasPrefix(k, annotations.PromQLInfo.Error())
				hasWarnPrefix := strings.HasPrefix(k, annotations.PromQLWarning.Error())
				return hasInfoPrefix || hasWarnPrefix
			}
			maps.DeleteFunc(l, discardPromqlAnnotations)
			maps.DeleteFunc(r, discardPromqlAnnotations)

			if len(l) != len(r) {
				return false
			}
			for k, v := range l {
				if !cmp.Equal(r[k], v) {
					return false
				}
			}
			for k, v := range r {
				if !cmp.Equal(l[k], v) {
					return false
				}
			}
			return true
		}
		compareValueMetrics := func(l, r labels.Labels) (valueMetric bool, equals bool) {
			// For count_value() float values embedded in the labels should be extracted out and compared separately from other labels.
			lLabels := l.Copy()
			rLabels := r.Copy()
			var (
				lVal, rVal     string
				lFloat, rFloat float64
				err            error
			)

			if lVal = lLabels.Get("value"); lVal == "" {
				return false, false
			}

			if rVal = rLabels.Get("value"); rVal == "" {
				return false, false
			}

			if lFloat, err = strconv.ParseFloat(lVal, 64); err != nil {
				return false, false
			}
			if rFloat, err = strconv.ParseFloat(rVal, 64); err != nil {
				return false, false
			}

			// Exclude the value label in comparison.
			lLabels = lLabels.MatchLabels(false, "value")
			rLabels = rLabels.MatchLabels(false, "value")

			if !labels.Equal(lLabels, rLabels) {
				return false, false
			}

			return true, compareFloats(lFloat, rFloat)
		}
		compareMetrics := func(l, r labels.Labels) bool {
			if valueMetric, equals := compareValueMetrics(l, r); valueMetric {
				return equals
			}
			return l.Hash() == r.Hash()
		}

		compareErrors := func(l, r error) (stop bool, result bool) {
			if l == nil && r == nil {
				return false, true
			}
			// If both have errors, consider them equal - error messages may differ
			// between engines (e.g., remote exec wrapper, different series ordering)
			// but what matters is that both produced an error.
			if l != nil && r != nil {
				return true, true
			}
			err := l
			if err == nil {
				err = r
			}
			// Thanos engine handles duplicate label check differently than Prometheus engine.
			return true, err.Error() == extlabels.ErrDuplicateLabelSet.Error()
		}

		if stop, result := compareErrors(x.Err, y.Err); stop {
			return result
		}

		if !compareAnnotations(x.Warnings, y.Warnings) {
			return false
		}

		vx, xvec := x.Value.(promql.Vector)
		vy, yvec := y.Value.(promql.Vector)

		if xvec && yvec {
			if len(vx) != len(vy) {
				return false
			}

			// Sort vector before comparing.
			sort.Sort(samplesByLabels(vx))
			sort.Sort(samplesByLabels(vy))

			for i := range vx {
				if !compareMetrics(vx[i].Metric, vy[i].Metric) {
					return false
				}
				if vx[i].T != vy[i].T {
					return false
				}
				if !compareFloats(vx[i].F, vy[i].F) {
					return false
				}
				if !compareHistograms(vx[i].H, vy[i].H) {
					return false
				}
			}
			return true
		}

		mx, xmat := x.Value.(promql.Matrix)
		my, ymat := y.Value.(promql.Matrix)

		if xmat && ymat {
			if len(mx) != len(my) {
				return false
			}
			// Sort matrix before comparing.
			sort.Sort(seriesByLabels(mx))
			sort.Sort(seriesByLabels(my))
			for i := range mx {
				mxs := mx[i]
				mys := my[i]

				if !compareMetrics(mxs.Metric, mys.Metric) {
					return false
				}

				xps := mxs.Floats
				yps := mys.Floats

				if len(xps) != len(yps) {
					return false
				}
				for j := range xps {
					if xps[j].T != yps[j].T {
						return false
					}
					if !compareFloats(xps[j].F, yps[j].F) {
						return false
					}
				}
				xph := mxs.Histograms
				yph := mys.Histograms

				if len(xph) != len(yph) {
					return false
				}
				for j := range xph {
					if xph[j].T != yph[j].T {
						return false
					}
					if !compareHistograms(xph[j].H, yph[j].H) {
						return false
					}
				}
			}
			return true
		}

		sx, xscalar := x.Value.(promql.Scalar)
		sy, yscalar := y.Value.(promql.Scalar)
		if xscalar && yscalar {
			if sx.T != sy.T {
				return false
			}
			return compareFloats(sx.V, sy.V)
		}
		return false
	})

	samplesComparer = cmp.Comparer(func(x, y *stats.QuerySamples) bool {
		if x == nil && y == nil {
			return true
		}
		if x.TotalSamples != y.TotalSamples {
			return false
		}

		if !cmp.Equal(x.TotalSamplesPerStep, y.TotalSamplesPerStep) {
			return false
		}

		if !cmp.Equal(x.TotalSamplesPerStepMap(), y.TotalSamplesPerStepMap()) {
			return false
		}
		return true
	})
)

func queryExplanation(q promql.Query) string {
	eq, ok := q.(engine.ExplainableQuery)
	if !ok {
		return ""
	}

	var explain func(w io.Writer, n engine.ExplainOutputNode, indent, indentNext string)

	explain = func(w io.Writer, n engine.ExplainOutputNode, indent, indentNext string) {
		next := n.Children
		me := n.OperatorName

		_, _ = w.Write([]byte(indent))
		_, _ = w.Write([]byte(me))
		if len(next) == 0 {
			_, _ = w.Write([]byte("\n"))
			return
		}

		if me == "[*CancellableOperator]" {
			_, _ = w.Write([]byte(": "))
			explain(w, next[0], "", indentNext)
			return
		}
		_, _ = w.Write([]byte(":\n"))

		for i, n := range next {
			if i == len(next)-1 {
				explain(w, n, indentNext+"└──", indentNext+"   ")
			} else {
				explain(w, n, indentNext+"├──", indentNext+"│  ")
			}
		}
	}

	var b bytes.Buffer
	explain(&b, *eq.Explain(), "", "")

	return fmt.Sprintf("Query: %s\nExplanation:\n%s\n", q.String(), b.String())
}

// Adapted from: https://github.com/prometheus/prometheus/blob/906f6a33b60cec2596018ac8cc97ac41b16b06b7/promql/promqltest/testdata/functions.test#L814
func TestDoubleExponentialSmoothing(t *testing.T) {
	t.Parallel()

	const (
		testTimeout    = 1 * time.Hour
		testMaxSamples = math.MaxInt64
		testQueryStart = 0
		testQueryEnd   = 3600
		testQueryStep  = 30
	)

	defaultStart := time.Unix(testQueryStart, 0)
	defaultEnd := time.Unix(testQueryEnd, 0)
	defaultStep := testQueryStep * time.Second

	cases := []struct {
		name string

		load  string
		query string

		start time.Time
		end   time.Time
		step  time.Duration
	}{
		{
			name: "double exponential smoothing basic",
			load: `load 30s
			    http_requests_total{pod="nginx-1"} 1+1x15
			    http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `double_exponential_smoothing(http_requests_total[5m], 0.1, 0.1)`,
		},
		{
			name: "double exponential smoothing with positive trend",
			load: `load 10s
			    http_requests{job="api-server", instance="0", group="production"}    0+10x1000 100+30x1000
			    http_requests{job="api-server", instance="1", group="production"}    0+20x1000 200+30x1000`,
			query: `double_exponential_smoothing(http_requests[5m], 0.01, 0.1)`,
		},
		{
			name: "double exponential smoothing with negative trend",
			load: `load 10s
			    http_requests{job="api-server", instance="0", group="production"}    8000-10x1000
			    http_requests{job="api-server", instance="1", group="production"}    0-20x1000`,
			query: `double_exponential_smoothing(http_requests[5m], 0.01, 0.1)`,
		},
		{
			name: "double exponential smoothing with mixed histogram data",
			load: `load 30s
			    http_requests_mix{job="api-server", instance="0"} 0+10x1000 100+30x1000 {{schema:0 count:1 sum:2}}x1000
			    http_requests_mix{job="api-server", instance="1"} 0+20x1000 200+30x1000 {{schema:0 count:1 sum:2}}x1000`,
			query: `double_exponential_smoothing(http_requests_mix[5m], 0.01, 0.1)`,
		},
		{
			name: "double exponential smoothing with pure histogram data",
			load: `load 30s
			    http_requests_histogram{job="api-server", instance="1"} {{schema:0 count:1 sum:2}}x1000`,
			query: `double_exponential_smoothing(http_requests_histogram[5m], 0.01, 0.1)`,
		},
	}

	for _, tcase := range cases {
		t.Run(tcase.name, func(t *testing.T) {
			t.Parallel()

			storage := promqltest.LoadedStorage(t, tcase.load)
			defer storage.Close()

			opts := promql.EngineOpts{
				Timeout:              testTimeout,
				MaxSamples:           testMaxSamples,
				EnableNegativeOffset: true,
				EnableAtModifier:     true,
			}

			start := defaultStart
			if !tcase.start.IsZero() {
				start = tcase.start
			}
			end := defaultEnd
			if !tcase.end.IsZero() {
				end = tcase.end
			}
			step := defaultStep
			if tcase.step != 0 {
				step = tcase.step
			}

			ctx := context.Background()
			oldEngine := promql.NewEngine(opts)
			q1, err := oldEngine.NewRangeQuery(ctx, storage, nil, tcase.query, start, end, step)
			testutil.Ok(t, errors.Wrap(err, "create old engine range query"))
			oldResult := q1.Exec(ctx)

			newEngine := engine.New(engine.Opts{EngineOpts: opts})
			q2, err := newEngine.NewRangeQuery(ctx, storage, nil, tcase.query, start, end, step)
			testutil.Ok(t, errors.Wrap(err, "create new engine range query"))
			newResult := q2.Exec(ctx)

			testutil.WithGoCmp(comparer).Equals(t, oldResult, newResult, queryExplanation(q2))
		})
	}
}
