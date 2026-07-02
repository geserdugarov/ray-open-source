#!/usr/bin/env bash
#
# Verify a wheelhouse will install the intended pylance build.
#
# Guards against the failure modes that silently shipped a stale Lance to the
# cluster (installed pylance 4.0.0 instead of the local 7.0.0, so
# `dataset.prewarm_index(partition_ids=...)` was missing):
#
#   1. More than one pylance wheel present -> `pip install --find-links pylance`
#      resolves by name and may prefer the wrong one (higher version, or a
#      more-specific tag).
#   2. A lance-namespace wheel >=0.8 present -> conflicts with the pylance 7.0.0
#      wheel's lance-namespace upper bound (`<0.8`), so `pip install --no-index`
#      cannot satisfy pylance 7.0.0 offline and silently backtracks to an older
#      pylance wheel. (The internal cluster pylance pins an exact pre-release,
#      `lance-namespace==0.7.7rc0+h0.cbu.mrs.370.r1`; requirements.txt uses
#      `>=0.7.7rc0,<0.8` to admit it. A `0.7.7rc0+...` wheel is below 0.8 so it
#      is NOT flagged here.)
#
# It also confirms the single pylance wheel actually exposes the
# partition_ids prewarm API (catches a wheel built from a too-old commit).
#
# Usage: verify_wheelhouse.sh [WHEELHOUSE]   (default: $WHEELHOUSE or ~/wheelhouse)

set -euo pipefail

WHEELHOUSE="${1:-${WHEELHOUSE:-$HOME/wheelhouse}}"

log()  { printf '[verify-wheelhouse] %s\n' "$*"; }
fail() { printf '[verify-wheelhouse] FAIL: %s\n' "$*" >&2; exit 1; }

[[ -d "$WHEELHOUSE" ]] || fail "wheelhouse not found: $WHEELHOUSE"

shopt -s nullglob

# 1. Exactly one pylance wheel.
pylance=( "$WHEELHOUSE"/pylance-*.whl )
(( ${#pylance[@]} == 1 )) \
  || fail "expected exactly one pylance wheel in $WHEELHOUSE, found ${#pylance[@]}: ${pylance[*]##*/}"
log "ok: single pylance wheel: ${pylance[0]##*/}"

# 2. No lance-namespace >=0.8 (pylance 7.0.0 pins <0.8); at least one present.
bad_ns=( "$WHEELHOUSE"/lance_namespace-0.8*.whl \
         "$WHEELHOUSE"/lance_namespace-0.9*.whl \
         "$WHEELHOUSE"/lance_namespace-[1-9]*.whl )
(( ${#bad_ns[@]} == 0 )) \
  || fail "incompatible lance-namespace wheel(s) present (pylance needs <0.8): ${bad_ns[*]##*/}"
ns=( "$WHEELHOUSE"/lance_namespace-*.whl )
(( ${#ns[@]} >= 1 )) \
  || fail "no lance-namespace wheel staged (pylance needs >=0.7.7rc0,<0.8)"
log "ok: lance-namespace wheel(s): ${ns[*]##*/}"

# 2b. At most one ray wheel. A locally built ray (e.g. the 2.53.0 branch build)
# sitting next to a stock PyPI ray pulled in by `pip wheel -r requirements.txt`
# lets `pip install --no-index ray` silently prefer the higher version — the same
# wrong-version trap as duplicate pylance wheels. Ray is optional here (nodes may
# install it another way), so zero is allowed; two or more is a hard failure.
ray_wheels=( "$WHEELHOUSE"/ray-*.whl )
if (( ${#ray_wheels[@]} > 1 )); then
  fail "more than one ray wheel in $WHEELHOUSE (silent version backtrack risk): ${ray_wheels[*]##*/}"
elif (( ${#ray_wheels[@]} == 1 )); then
  log "ok: single ray wheel: ${ray_wheels[0]##*/}"
else
  log "note: no ray wheel staged (nodes must install Ray another way)"
fi

# 3. The pylance wheel exposes the partition_ids prewarm API.
# Capture dataset.py fully before matching. A naive `unzip -p ... | grep -q` lets
# grep exit on the first match and close the pipe, so unzip dies with SIGPIPE (141);
# under `set -o pipefail` that 141 becomes the pipeline's exit status and the check
# false-negatives on a *good* wheel (the API is early in a large file). Command
# substitution reads unzip to EOF (no SIGPIPE) and the bash glob match needs no pipe.
dataset_py="$(unzip -p "${pylance[0]}" 'lance/dataset.py' 2>/dev/null || true)"
if [[ "$dataset_py" == *partition_ids* ]]; then
  log "ok: pylance wheel exposes dataset.prewarm_index(partition_ids=...)"
else
  fail "pylance wheel ${pylance[0]##*/} has NO partition_ids in lance/dataset.py (built from a too-old commit?)"
fi

shopt -u nullglob
log "wheelhouse OK: $WHEELHOUSE"
