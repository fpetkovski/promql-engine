# Claude Instructions

## Fork-Specific Files

This repository is a fork of `thanos-io/promql-engine`.

When creating pull requests against the upstream repository, **exclude** the following fork-specific files:

- `.github/workflows/claude-code-review.yml`
- `.github/workflows/claude.yml`
- `CLAUDE.md`

These files are specific to this fork and should not be included in upstream contributions.
Also remove those files from your branch before pushing it to Github.
