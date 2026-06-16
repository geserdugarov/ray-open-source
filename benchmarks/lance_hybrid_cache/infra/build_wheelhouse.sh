#!/usr/bin/env bash
#
# Build a clean wheelhouse for the Lance hybrid-cache benchmark (local deploy).
#
# Produces a wheelhouse containing EXACTLY ONE pylance wheel (freshly built
# from the local lance-open-source checkout), an optional Ray wheel, and every
# requirements.txt dependency. Stale wheels are purged FIRST so a leftover
# build can never be silently preferred at install time.
#
# Why the clean stage exists: `maturin build --out dist` never cleans dist/,
# and `cp pylance-*.whl` + `rsync --delete` faithfully propagate whatever is
# there. A leftover older pylance (e.g. 4.0.0) combined with a too-loose
# lance-namespace pin made `pip install --no-index` silently backtrack to the
# old pylance, installing a build with no
# `dataset.prewarm_index(partition_ids=...)`. See REAL_CLUSTER.md.
#
# Run this on the dev/build machine inside the Ray venv. For the real-cluster
# deploy, run it, then `infra/ship_real_cluster.sh` (which re-verifies the
# wheelhouse before rsyncing).
#
# Overridable env vars:
#   LANCE_CHECKOUT  (default ~/git/lance-open-source)
#   RAY_CHECKOUT    (default ~/git/ray-open-source)
#   WHEELHOUSE      (default ~/wheelhouse)
#   LANCE_DIST      (default $LANCE_CHECKOUT/python/dist)
#   RAY_WHEEL_GLOB  (default $RAY_CHECKOUT/dist/ray-*.whl; skipped if no match)

set -euo pipefail

LANCE_CHECKOUT="${LANCE_CHECKOUT:-$HOME/git/lance-open-source}"
RAY_CHECKOUT="${RAY_CHECKOUT:-$HOME/git/ray-open-source}"
WHEELHOUSE="${WHEELHOUSE:-$HOME/wheelhouse}"
LANCE_DIST="${LANCE_DIST:-$LANCE_CHECKOUT/python/dist}"
RAY_WHEEL_GLOB="${RAY_WHEEL_GLOB:-$RAY_CHECKOUT/dist/ray-*.whl}"

BENCH_DIR="$RAY_CHECKOUT/benchmarks/lance_hybrid_cache"
REQS="$BENCH_DIR/requirements.txt"

log()  { printf '[build-wheelhouse] %s\n' "$*"; }
fail() { printf '[build-wheelhouse] FAIL: %s\n' "$*" >&2; exit 1; }

command -v maturin >/dev/null || fail "maturin not on PATH (activate the Ray venv first)"
command -v pip     >/dev/null || fail "pip not on PATH (activate the Ray venv first)"
[[ -d "$LANCE_CHECKOUT/python" ]] || fail "missing lance checkout: $LANCE_CHECKOUT/python"
[[ -f "$REQS" ]] || fail "missing requirements.txt: $REQS"

mkdir -p "$WHEELHOUSE" "$LANCE_DIST"

# --- Stage 1: clean stale wheels --------------------------------------------
# Remove every previously built/staged pylance and lance-namespace wheel so a
# stale version cannot survive into the wheelhouse and get preferred by pip.
log "clean: purging stale pylance / lance-namespace wheels from dist + wheelhouse"
rm -fv "$LANCE_DIST"/pylance-*.whl \
       "$WHEELHOUSE"/pylance-*.whl \
       "$WHEELHOUSE"/lance_namespace-*.whl \
       "$WHEELHOUSE"/lance-namespace-*.whl 2>/dev/null || true

# --- Stage 2: build pylance from the local checkout -------------------------
log "build: maturin build --release (pylance) from $LANCE_CHECKOUT/python"
( cd "$LANCE_CHECKOUT/python" && maturin build --release --out "$LANCE_DIST" )

# --- Stage 3: pre-resolve every dependency into the wheelhouse ---------------
# Uses the corrected lance-namespace pin in requirements.txt (>=0.7.7,<0.8),
# so the staged lance-namespace is compatible with the pylance 7.0.0 wheel.
log "deps: pip wheel -r requirements.txt -> $WHEELHOUSE"
pip wheel -r "$REQS" -w "$WHEELHOUSE"

# --- Stage 4: stage the freshly built local wheels --------------------------
shopt -s nullglob
# shellcheck disable=SC2206  # intentional: RAY_WHEEL_GLOB is a glob pattern to expand
ray_wheels=( $RAY_WHEEL_GLOB )
if (( ${#ray_wheels[@]} )); then
  # `pip wheel -r requirements.txt` (Stage 3) resolves `ray[default]` and stages a
  # *stock PyPI* ray wheel. Drop it before copying the local build, otherwise both
  # coexist and `pip install --no-index ray` silently prefers the higher version
  # (same backtrack trap as duplicate pylance wheels; verify_wheelhouse.sh guards it).
  log "stage: purging any pre-staged (stock) ray wheel so only the local build remains"
  rm -fv "$WHEELHOUSE"/ray-*.whl 2>/dev/null || true
  log "stage: copying Ray wheel(s): ${ray_wheels[*]##*/}"
  cp "${ray_wheels[@]}" "$WHEELHOUSE/"
else
  log "stage: no Ray wheel matched $RAY_WHEEL_GLOB"
  log "stage: build it with 'pip wheel . -w dist --no-deps' in $RAY_CHECKOUT/python if the nodes need it"
fi

pylance_built=( "$LANCE_DIST"/pylance-*.whl )
(( ${#pylance_built[@]} == 1 )) \
  || fail "expected exactly one freshly built pylance wheel in $LANCE_DIST, found ${#pylance_built[@]}: ${pylance_built[*]:-<none>}"
log "stage: copying pylance wheel: ${pylance_built[0]##*/}"
cp "${pylance_built[0]}" "$WHEELHOUSE/"
shopt -u nullglob

# --- Stage 5: verify the wheelhouse is sane ---------------------------------
bash "$BENCH_DIR/infra/verify_wheelhouse.sh" "$WHEELHOUSE"

log "done: clean wheelhouse ready at $WHEELHOUSE"
