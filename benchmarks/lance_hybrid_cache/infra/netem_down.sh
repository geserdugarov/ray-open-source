#!/usr/bin/env bash
# Remove the loopback qdisc installed by netem_up.sh.
set -euo pipefail

DEV="${DEV:-lo}"

if [[ $EUID -ne 0 ]]; then
  exec sudo -E DEV="$DEV" bash "$0" "$@"
fi

tc qdisc del dev "${DEV}" root 2>/dev/null || true
echo "[netem] cleared qdisc on ${DEV}"
tc qdisc show dev "${DEV}"
