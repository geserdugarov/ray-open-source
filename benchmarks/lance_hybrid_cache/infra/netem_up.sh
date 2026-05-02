#!/usr/bin/env bash
# Apply a 15 ms delay to loopback traffic destined for MinIO (port 9000) ONLY.
#
# Do NOT run `tc qdisc add dev lo root netem delay 15ms` — that slows ALL
# loopback, including Ray actor IPC, and pollutes the benchmark uniformly
# across scenarios.
#
# Implementation: prio qdisc splits traffic into bands; a u32 filter sends
# packets with dport 9000 into band 3, which carries a netem child. Bands
# 1 and 2 remain unaffected.
set -euo pipefail

DEV="${DEV:-lo}"
DELAY_MS="${DELAY_MS:-15}"
PORT="${PORT:-9000}"

if [[ $EUID -ne 0 ]]; then
  echo "[netem] re-invoking under sudo" >&2
  exec sudo -E DEV="$DEV" DELAY_MS="$DELAY_MS" PORT="$PORT" bash "$0" "$@"
fi

# Clear any prior qdisc on the device
tc qdisc del dev "${DEV}" root 2>/dev/null || true

# prio qdisc with 3 bands
tc qdisc add dev "${DEV}" root handle 1: prio bands 3

# netem child on band 3 (handle 30:)
tc qdisc add dev "${DEV}" parent 1:3 handle 30: netem delay "${DELAY_MS}ms"

# Send outbound packets with dport=PORT into band 3
tc filter add dev "${DEV}" parent 1:0 protocol ip prio 1 u32 \
  match ip dport "${PORT}" 0xffff flowid 1:3

# And inbound reply traffic (sport=PORT) for symmetric delay
tc filter add dev "${DEV}" parent 1:0 protocol ip prio 1 u32 \
  match ip sport "${PORT}" 0xffff flowid 1:3

echo "[netem] applied ${DELAY_MS}ms delay on ${DEV} for port ${PORT}"
tc qdisc show dev "${DEV}"
tc filter show dev "${DEV}"
