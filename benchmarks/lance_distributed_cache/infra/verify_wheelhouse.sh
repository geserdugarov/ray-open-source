#!/usr/bin/env bash
# Quick artifact checks; deployment also validates the installed Python API.
set -euo pipefail
WHEELHOUSE=
if [[ "${1:-}" == --lance-wheel && $# == 2 ]]; then
  wheel="$2"
elif [[ $# == 1 && "$1" != -* ]]; then
  WHEELHOUSE="$1"
  shopt -s nullglob
  pylance=("$WHEELHOUSE"/pylance-*.whl)
  ray=("$WHEELHOUSE"/ray-*.whl)
  if (( ${#pylance[@]} != 1 || ${#ray[@]} != 1 )); then
    echo "Expected exactly one pylance wheel and one Ray wheel in $WHEELHOUSE" >&2
    exit 1
  fi
  wheel="${pylance[0]}"
else
  echo "Usage: $0 WHEELHOUSE | --lance-wheel WHEEL" >&2
  exit 2
fi
entries="$(unzip -Z1 "$wheel")"
while IFS= read -r entry; do
  case "$entry" in
    pylance.pth|__editable__*pylance*.pth|*.data/purelib/pylance.pth|*.data/platlib/pylance.pth)
      printf 'Editable pylance wheel cannot be deployed: %s\n' "$wheel" >&2
      printf '%s references a source checkout instead of packaging Lance.\n' "$entry" >&2
      echo "Build a regular wheel from lance-open-source/python:" >&2
      echo "  uv run --frozen maturin build --profile release-with-debug --out dist/cluster" >&2
      echo "Set LANCE_WHEEL to the resulting wheel and rerun infra/build_wheelhouse.sh." >&2
      exit 1
      ;;
  esac
done <<< "$entries"
read_wheel_file() {
  local requested="$1" entry
  local matches=()
  while IFS= read -r entry; do
    # Wheels may install Python files from the root or a .data library tree.
    case "$entry" in
      "$requested"|*.data/purelib/"$requested"|*.data/platlib/"$requested")
        matches+=("$entry") ;;
    esac
  done <<< "$entries"
  if (( ${#matches[@]} != 1 )); then
    printf 'Expected one installable %s in %s; found %s.\n' \
      "$requested" "$wheel" "${#matches[@]}" >&2
    echo "Check the wheel contents and set LANCE_WHEEL to the intended Lance 12 build." >&2
    return 1
  fi
  unzip -p "$wheel" "${matches[0]}"
}
dataset="$(read_wheel_file lance/dataset.py)"
bindings="$(read_wheel_file lance/lance/__init__.pyi)"
if [[ "$dataset" != *partition_ids* || "$bindings" != *l1_partition_capacity_bytes* ]]; then
  printf 'Wheel is missing the Lance 12 two-level cache API: %s\n' "$wheel" >&2
  exit 1
fi
if [[ "$bindings" != *index_cache_stats* ]]; then
  printf 'Wheel is missing Session.index_cache_stats: %s\n' "$wheel" >&2
  echo "Rebuild Lance with the cache-counter changes, then rebuild the wheelhouse." >&2
  exit 1
fi
if [[ -n "$WHEELHOUSE" && -f "$WHEELHOUSE/SHA256SUMS" ]]; then
  (cd -- "$WHEELHOUSE" && sha256sum --check --quiet SHA256SUMS)
fi
if [[ -n "$WHEELHOUSE" ]]; then
  printf 'Wheelhouse OK: %s\n' "$WHEELHOUSE"
else
  printf 'Pylance wheel OK: %s\n' "$wheel"
fi
