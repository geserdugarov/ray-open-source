#!/usr/bin/env bash
# Resolve prebuilt Lance + Ray and their dependencies together for offline use.
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/real_cluster_common.sh"

if [[ -z "${LANCE_WHEEL:-}" ]]; then
  LANCE_CHECKOUT="${LANCE_CHECKOUT:-$BENCH_DIR/../../../lance-open-source}"
  shopt -s nullglob
  wheels=("$LANCE_CHECKOUT"/python/dist/pylance-*.whl)
  if (( ${#wheels[@]} != 1 )); then
    echo "Set LANCE_WHEEL to the exact locally built Lance 12 wheel" >&2
    exit 1
  fi
  LANCE_WHEEL="${wheels[0]}"
fi
[[ -f "$LANCE_WHEEL" ]] || { echo "Missing wheel: $LANCE_WHEEL" >&2; exit 1; }
printf 'Using pylance wheel: %s\n' "$LANCE_WHEEL"
# Reject editable/incomplete wheels before downloading any dependencies.
bash "$INFRA_DIR/verify_wheelhouse.sh" --lance-wheel "$LANCE_WHEEL"

BUILD_PYTHON="${BUILD_PYTHON:-$BENCH_DIR/out/build-venv/bin/python}"
if [[ ! -x "$BUILD_PYTHON" ]]; then
  "$PYTHON_BIN" -m venv "$(dirname -- "$(dirname -- "$BUILD_PYTHON")")"
fi
"$BUILD_PYTHON" -c 'import sys; assert sys.prefix != sys.base_prefix, "BUILD_PYTHON must be a virtual environment"'

mkdir -p "$WHEELHOUSE"
staging="$(mktemp -d "$WHEELHOUSE/.staging.XXXXXX")"
finish() {
  local status=$?
  if (( status == 0 )); then
    rm -r -- "$staging"
  else
    printf 'Build failed; downloaded artifacts retained at %s\n' "$staging" >&2
  fi
}
trap finish EXIT
ray_args=()
if [[ -n "${RAY_WHEEL:-}" ]]; then
  [[ -f "$RAY_WHEEL" ]] || { echo "Missing wheel: $RAY_WHEEL" >&2; exit 1; }
  ray_args=("${RAY_WHEEL}[default]")
fi
"$BUILD_PYTHON" -m pip download --only-binary=:all: --dest "$staging" \
  -r "$BENCH_DIR/requirements.txt" "$LANCE_WHEEL" "${ray_args[@]}"
bash "$INFRA_DIR/verify_wheelhouse.sh" "$staging"
(cd -- "$staging" && sha256sum ./*.whl > SHA256SUMS)

# Replace staged wheels only after the complete dependency resolution succeeds.
rm -f -- "$WHEELHOUSE"/*.whl
cp -- "$staging"/*.whl "$staging/SHA256SUMS" "$WHEELHOUSE/"
bash "$INFRA_DIR/verify_wheelhouse.sh" "$WHEELHOUSE"
