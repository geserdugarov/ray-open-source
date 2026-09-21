#!/usr/bin/env bash
# Delay only MinIO traffic. Review before use: this changes the host qdisc.
set -euo pipefail

DEV="${DEV:-lo}"
DELAY_MS="${DELAY_MS:-15}"
PORT="${PORT:-9000}"

if [[ ${EUID} -ne 0 ]]; then
  exec sudo -E DEV="${DEV}" DELAY_MS="${DELAY_MS}" PORT="${PORT}" bash "$0" "$@"
fi

tc qdisc del dev "${DEV}" root 2>/dev/null || true
tc qdisc add dev "${DEV}" root handle 1: prio bands 3
tc qdisc add dev "${DEV}" parent 1:3 handle 30: netem delay "${DELAY_MS}ms"
tc filter add dev "${DEV}" parent 1:0 protocol ip prio 1 u32 \
  match ip dport "${PORT}" 0xffff flowid 1:3
tc filter add dev "${DEV}" parent 1:0 protocol ip prio 1 u32 \
  match ip sport "${PORT}" 0xffff flowid 1:3
tc qdisc show dev "${DEV}"
