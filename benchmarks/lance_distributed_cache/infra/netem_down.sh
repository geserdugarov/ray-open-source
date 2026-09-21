#!/usr/bin/env bash
set -euo pipefail

DEV="${DEV:-lo}"
if [[ ${EUID} -ne 0 ]]; then
  exec sudo -E DEV="${DEV}" bash "$0" "$@"
fi
tc qdisc del dev "${DEV}" root 2>/dev/null || true
tc qdisc show dev "${DEV}"
