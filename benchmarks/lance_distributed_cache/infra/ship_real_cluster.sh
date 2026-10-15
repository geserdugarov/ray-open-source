#!/usr/bin/env bash
# Ship artifacts and install them into node-local venvs. No remote builds.
set -euo pipefail
if [[ "${1:-}" == --help ]]; then
  echo "Usage: $0 [--code-only] (settings: infra/real_cluster.env or CLUSTER_ENV)"
  exit 0
fi
if (( $# > 1 )) || [[ -n "${1:-}" && "$1" != --code-only ]]; then
  echo "Usage: $0 [--code-only]" >&2
  exit 2
fi
source "$(dirname -- "${BASH_SOURCE[0]}")/real_cluster_common.sh"
cluster_settings
code_only="${1:-}"
if [[ "$code_only" != --code-only ]]; then
  if ! bash "$INFRA_DIR/verify_wheelhouse.sh" "$WHEELHOUSE"; then
    echo "Run infra/build_wheelhouse.sh successfully before shipping." >&2
    exit 1
  fi
fi

for host in "$COORD_SSH_HOST" "$ACTOR0_SSH_HOST" "$ACTOR1_SSH_HOST"; do
  printf 'Deploying to %s@%s\n' "$SSH_USER" "$host"
  remote "$host" mkdir -p "$REMOTE_BENCH_DIR" "$REMOTE_WHEELHOUSE"
  rsync -az --delete --protect-args -e 'ssh -o BatchMode=yes' \
    --exclude '/.venv/' --exclude '/out/' --exclude '/results/' \
    --exclude '__pycache__/' --exclude '*.pyc' --exclude '/infra/real_cluster.env' \
    "$BENCH_DIR/" "$SSH_USER@$host:$REMOTE_BENCH_DIR/"
  if [[ "$code_only" == --code-only ]]; then
    continue
  fi
  rsync -a --checksum --delete --protect-args -e 'ssh -o BatchMode=yes' \
    "$WHEELHOUSE/" "$SSH_USER@$host:$REMOTE_WHEELHOUSE/"
  remote "$host" "$PYTHON_BIN" -m venv "$REMOTE_VENV"
  remote "$host" bash -s -- "$REMOTE_BENCH_DIR" "$REMOTE_WHEELHOUSE" <<'REMOTE'
set -euo pipefail
bench="$1"
wheelhouse="$2"
pylance=("$wheelhouse"/pylance-*.whl)
ray=("$wheelhouse"/ray-*.whl)
"$bench/.venv/bin/python" -m pip install --no-index --only-binary=:all: \
  --find-links="$wheelhouse" --force-reinstall -r "$bench/requirements.txt" \
  "${pylance[0]}" "${ray[0]}[default]"
cd -- "$bench"
"$bench/.venv/bin/python" -c 'import lance, ray; from run_benchmark import _validate_lance_api; _validate_lance_api(lance); print("Installed:", "lance", lance.__version__, "ray", ray.__version__)'
REMOTE
done

# MinIO needs only Docker Compose and the infra files, not a Python venv.
remote "$MINIO_SSH_HOST" mkdir -p "$REMOTE_BENCH_DIR/infra"
rsync -az --delete --protect-args -e 'ssh -o BatchMode=yes' \
  --exclude '/real_cluster.env' \
  "$INFRA_DIR/" "$SSH_USER@$MINIO_SSH_HOST:$REMOTE_BENCH_DIR/infra/"
echo "Deploy complete. Run infra/run_real_cluster.sh start, minio, then smoke."
