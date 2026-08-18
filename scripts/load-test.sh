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
#   🔴 AND THE PREFLIGHT MUST NOT AIM AT THE SYSTEM UNDER TEST. The first version of
#   this script fired preflight at /events, which is exactly wrong: if the service
#   is the bottleneck then the "generator ceiling" it reports is the SERVICE's
#   ceiling, and the two numbers become the same measurement with different names --
#   guaranteeing they agree and proving nothing. Preflight therefore aims at
#   --preflight-url, which defaults to the load-test sink: an nginx that returns 200
#   and does no work, no queue, no database. Whatever rate comes back is this
#   laptop's own limit against this network.
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
SUBSCRIBER=""
PREFLIGHT=0
PREFLIGHT_URL=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rate)       RATE="$2"; shift 2 ;;
    --duration)   DURATION="$2"; shift 2 ;;
    --url)        URL="$2"; shift 2 ;;
    --subscriber) SUBSCRIBER="$2"; shift 2 ;;
    --preflight)  PREFLIGHT=1; shift ;;
    --preflight-url) PREFLIGHT_URL="$2"; shift 2 ;;
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
  if [[ -z "$PREFLIGHT_URL" ]]; then
    echo "--preflight needs --preflight-url pointing at something that does NO WORK" >&2
    echo "(the load-test sink: an nginx returning 200). Aiming it at /events would" >&2
    echo "measure the service and call the result a generator ceiling." >&2
    exit 2
  fi
  echo "PREFLIGHT: firing unpaced for 5s at a do-nothing endpoint to find THIS"
  echo "           machine's ceiling. Target: $PREFLIGHT_URL"
  start=$(python3 -c 'import time;print(time.time())')
  count=0
  end_by=$(python3 -c "import time;print(time.time()+5)")
  while (( $(python3 -c "import time;print(1 if time.time() < $end_by else 0)") )); do
    for _ in $(seq 1 30); do
      curl -s -o /dev/null -m 10 -X POST "$PREFLIGHT_URL" -d '{}' &
    done
    wait
    count=$((count + 30))
  done
  elapsed=$(python3 -c "import time;print(round(time.time()-$start,2))")
  CEIL=$(python3 -c "print(round($count/$elapsed,1))")
  echo
  echo "  GENERATOR CEILING: $count requests in ${elapsed}s = ${CEIL} req/s"
  echo
  echo "  ⚠️  Any load-test result within ~30% of ${CEIL}/s is suspect: at that point"
  echo "      this script is a plausible bottleneck and the number may describe the"
  echo "      generator rather than the service."
  exit 0
fi

if [[ -z "$SUBSCRIBER" ]]; then
  echo "--subscriber is required (the URL workers deliver to)." >&2
  echo "It must be PUBLIC: the producer's SSRF gate rejects private ranges," >&2
  echo "including cluster DNS names and ClusterIPs. See load-test-receiver.yaml." >&2
  exit 2
fi

CONC="${CONC:-$RATE}"
echo "=============================================================="
echo " concurrency : ${CONC} workers, back-to-back, for ${DURATION}s"
echo " endpoint    : $URL/events"
echo " subscriber  : $SUBSCRIBER"
echo "=============================================================="

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# Pre-generate every payload BEFORE the clock starts. uuidgen and JSON assembly are
# real work, and doing them inside the timed loop measures this laptop's ability to
# build strings rather than the service's ability to accept them.
#
# ⚠️ Each event_id must be UNIQUE: the producer enforces idempotency and rejects a
# repeat within its window, so a load tool replaying one fixed body would measure the
# duplicate-rejection path at 100% and report it as success.
python3 - "$TMP" "$SUBSCRIBER" "$(( CONC * DURATION * 4 ))" <<'PYGEN'
import json, sys, uuid, os
tmp, sub, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
d = os.path.join(tmp, "p"); os.makedirs(d, exist_ok=True)
for i in range(n):
    with open(os.path.join(d, f"{i:06d}.json"), "w") as f:
        json.dump({"event_id": str(uuid.uuid4()), "subscriber_url": sub,
                   "payload": json.dumps({"i": i})}, f)
PYGEN
echo " payloads    : $(ls "$TMP/p" | wc -l | tr -d ' ') pre-generated"

START=$(python3 -c 'import time;print(time.time())')
END_BY=$(python3 -c "import time;print(time.time()+$DURATION)")

worker() {
  local id=$1
  while (( $(python3 -c "import time;print(1 if time.time() < $END_BY else 0)") )); do
    for f in $(ls "$TMP/p" | awk "NR % $CONC == $id" | head -25); do
      [[ -f "$TMP/p/$f" ]] || continue
      code=$(curl -s -o /dev/null -m 15 -w '%{http_code}' -X POST "$URL/events" \
        -H 'Content-Type: application/json' --data-binary "@$TMP/p/$f")
      echo "$code" >> "$TMP/codes"
      rm -f "$TMP/p/$f"
      (( $(python3 -c "import time;print(1 if time.time() < $END_BY else 0)") )) || break
    done
  done
}

for (( w=0; w<CONC; w++ )); do worker "$w" & done
wait

ELAPSED=$(python3 -c "import time;print(round(time.time()-$START,2))")
SENT=$(wc -l < "$TMP/codes" 2>/dev/null | tr -d ' '); SENT=${SENT:-0}
OK=$(grep -c '^202$' "$TMP/codes" 2>/dev/null || true); OK=${OK:-0}
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
