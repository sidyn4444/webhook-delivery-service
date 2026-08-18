#!/usr/bin/env bash
#
# load-test.sh -- drive POST /events at a target rate and report what the SYSTEM held.
#
#   ./scripts/load-test.sh --rate 20 --duration 60
#   ./scripts/load-test.sh --rate 40 --duration 60 --url https://webhooks.sndiaye.com
#
# WHY A SHELL SCRIPT AND NOT GATLING OR k6
#
#   The bottleneck under test is the WORKER POOL and REDIS, not an HTTP client.
#   A JVM load tool competing for the same laptop CPU is a confound, not a feature:
#   when the numbers flatten you cannot tell whether the system saturated or the
#   generator did. curl processes are cheap, independent, and easy to reason about.
#
#   ⚠️ The honest cost, stated rather than hidden: a shell generator has a LOWER
#   ceiling than a real load tool. Somewhere above a few hundred requests/sec the
#   process-spawn cost dominates and this script becomes the bottleneck. That is
#   why --preflight measures the generator's own ceiling FIRST (see below): a flat
#   throughput curve is equally consistent with a saturated system and a saturated
#   client, and the only way to tell them apart is to know the client's limit
#   before you start.
#
# WHAT IS ACTUALLY BEING MEASURED
#
#   NOT "how many 202s can I get". POST /events returns 202 the moment the job is
#   enqueued -- it accepted the event and promised nothing about when it will be
#   delivered. A system accepting 500/sec while delivering 200/sec returns 202 all
#   day and looks perfectly healthy from here.
#
#   🔴 Sustained throughput is the rate at which the QUEUE DEPTH STAYS FLAT. This
#   script therefore reports the accept rate AND tells you to read the backlog; the
#   accept rate alone is a measurement of this script, not of the service.

set -uo pipefail

URL="https://webhooks.sndiaye.com"
RATE=20
DURATION=60
SUBSCRIBER="https://httpbin.org/post"
PREFLIGHT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rate)       RATE="$2"; shift 2 ;;
    --duration)   DURATION="$2"; shift 2 ;;
    --url)        URL="$2"; shift 2 ;;
    --subscriber) SUBSCRIBER="$2"; shift 2 ;;
    --preflight)  PREFLIGHT=1; shift ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

# --------------------------------------------------------------------------
# PREFLIGHT -- measure THIS SCRIPT's ceiling before measuring the service.
#
# Fires as fast as it can with no pacing for 5 seconds. Whatever comes out is
# the generator's own maximum on this machine and network. If the load test's
# result is anywhere near this number, the test measured the generator.
# --------------------------------------------------------------------------
if [[ "$PREFLIGHT" == "1" ]]; then
  echo "PREFLIGHT: firing unpaced for 5s to find the generator's own ceiling..."
  start=$(python3 -c 'import time;print(time.time())')
  count=0
  end_by=$(python3 -c "import time;print(time.time()+5)")
  while (( $(python3 -c "import time;print(1 if time.time() < $end_by else 0)") )); do
    for _ in $(seq 1 20); do
      curl -s -o /dev/null -m 10 -X POST "$URL/events" \
        -H 'Content-Type: application/json' \
        -d "{\"event_id\":\"$(uuidgen)\",\"subscriber_url\":\"$SUBSCRIBER\",\"payload\":\"{}\"}" &
    done
    wait
    count=$((count + 20))
  done
  elapsed=$(python3 -c "import time;print(round(time.time()-$start,2))")
  echo "PREFLIGHT: $count requests in ${elapsed}s = $(python3 -c "print(round($count/$elapsed,1))") req/s generator ceiling"
  echo
  exit 0
fi

TOTAL=$(( RATE * DURATION ))
echo "=============================================================="
echo " target rate : ${RATE}/s for ${DURATION}s  (${TOTAL} events)"
echo " endpoint    : $URL/events"
echo " subscriber  : $SUBSCRIBER"
echo "=============================================================="

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

START=$(python3 -c 'import time;print(time.time())')

for (( sec=0; sec<DURATION; sec++ )); do
  slot_start=$(python3 -c 'import time;print(time.time())')

  for (( i=0; i<RATE; i++ )); do
    {
      code=$(curl -s -o /dev/null -m 15 -w '%{http_code}' -X POST "$URL/events" \
        -H 'Content-Type: application/json' \
        -d "{\"event_id\":\"$(uuidgen)\",\"subscriber_url\":\"$SUBSCRIBER\",\"payload\":\"{\\\"s\\\":$sec}\"}")
      echo "$code" >> "$TMP/codes"
    } &
  done
  wait

  # Pace to one batch per second. If the batch already took longer than a second
  # the generator is behind and cannot hold the requested rate -- reported at the
  # end rather than silently absorbed.
  python3 -c "
import time
rem = 1.0 - (time.time() - $slot_start)
if rem > 0: time.sleep(rem)
"
  printf '\r  %ds/%ds  sent=%s' "$((sec+1))" "$DURATION" "$(wc -l < "$TMP/codes" | tr -d ' ')"
done
echo

ELAPSED=$(python3 -c "import time;print(round(time.time()-$START,2))")
SENT=$(wc -l < "$TMP/codes" | tr -d ' ')
OK=$(grep -c '^202$' "$TMP/codes" || true)
BAD=$(( SENT - OK ))
ACTUAL=$(python3 -c "print(round($SENT/$ELAPSED,1))")

echo
echo "=============================================================="
echo " elapsed          : ${ELAPSED}s"
echo " sent             : $SENT"
echo " 202 accepted     : $OK"
echo " non-202          : $BAD"
echo " ACCEPT RATE      : ${ACTUAL}/s"
echo "=============================================================="
if [[ "$BAD" -gt 0 ]]; then
  echo " non-202 codes seen:"
  grep -v '^202$' "$TMP/codes" | sort | uniq -c | sed 's/^/   /'
fi
echo
echo " ⚠️  THIS IS THE ACCEPT RATE, NOT THE DELIVERY RATE."
echo "     /events returns 202 on enqueue. To find what the system HELD, read:"
echo "       max(webhook_queue_depth)                        <- must stay flat"
echo "       sum(rate(http_client_requests_seconds_count[1m]))  <- actual deliveries/sec"
