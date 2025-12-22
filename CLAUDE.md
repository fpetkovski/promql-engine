# Claude Instructions

## CRITICAL: Fork-Specific Files - Upstream PR Requirements

⚠️ **IMPORTANT**: This repository is a fork of `thanos-io/promql-engine`.

### Fork-Specific Files (NEVER include in upstream PRs)

These files exist ONLY in this fork:
- `.github/workflows/claude-code-review.yml`
- `.github/workflows/claude.yml`
- `CLAUDE.md`

### Workflow for Upstream Contributions

**When creating PRs to `thanos-io/promql-engine`, use this workflow:**

1. **Check if upstream is configured:**
   ```bash
   git remote -v
   ```
   If `upstream` doesn't exist, add it:
   ```bash
   git remote add upstream https://github.com/thanos-io/promql-engine.git
   git fetch upstream
   ```

2. **Create branch from upstream, NOT from fork's main:**
   ```bash
   git checkout -b feature-name upstream/main
   ```
   This ensures fork-specific files are never in your branch.

3. **Make your changes and commit normally**

4. **Push to your fork:**
   ```bash
   git push -u origin feature-name
   ```

5. **Create PR with base as `thanos-io/promql-engine:main`**

**CRITICAL: Never create upstream feature branches from the fork's `main` branch, as it contains fork-specific files!**

### If You Already Created a Branch from Fork's Main

DO NOT commit removals of fork-specific files. Instead, recreate the branch:

```bash
# Save your changes
git diff upstream/main > my-changes.patch

# Create new branch from upstream
git checkout -b feature-name-clean upstream/main

# Apply your changes
git apply my-changes.patch

# Commit and push
git add .
git commit -m "Your actual changes"
git push -u origin feature-name-clean
```

### Pre-PR Checklist for Upstream Contributions

- [ ] Branch was created from `upstream/main` (NOT `origin/main`)
- [ ] Verified no fork-specific files in branch: `git ls-files | grep -E "(claude.*\.yml|CLAUDE\.md)"`
- [ ] PR base is `thanos-io/promql-engine:main`
