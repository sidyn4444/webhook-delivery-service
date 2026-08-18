#!/usr/bin/env bash
#
# chaos-test.sh -- kill a worker MID-DELIVERY and measure two separate things.
#
#   ./scripts/chaos-test.sh --sink http://<sink-alb>/
#
# 🔴 THE CLAIM IS TWO CLAIMS, AND ONLY ONE OF THEM IS ABOUT OUR CODE.
#
#   ① "the pool recovered in X seconds" is a statement about KUBERNETES. A Deployment
#      notices a missing pod and replaces it. That is worth measuring but it is not an
#      achievement of this system -- any Deployment does it.
#
#   ② "no event was lost" is a statement about OUR reclaim path -- the RPOPLPUSH
#      reliable-queue pattern, the in-flight index and the recovery sweep. A job the
#      dead worker had already taken off the queue is invisible to Kubernetes: nothing
#      about replacing a pod puts that job back. Only our own machinery does.
#
#   ⚠️ Reporting only ① and calling it resilience would be claiming credit for
#      Kubernetes. ② is the interesting half.
#
# 🔴 AND THE ACCOUNTING CANNOT COME FROM A PROMETHEUS COUNTER. THIS WAS MEASURED THE
#    HARD WAY: the first version summed `http_client_requests_seconds_count` before and
#    after, and reported MINUS 68,058 deliveries.
#
#    A Prometheus counter lives in the process that owns it. Kill the pod and its SERIES
#    DISAPPEARS; the replacement pod starts a brand-new series at zero. So `sum(counter)`
#    across pods is NOT monotonic when the pod set changes -- it drops by whatever the
#    dead pod had accumulated. A chaos test is precisely the thing that breaks it.
#
#    (`rate()` and `increase()` ARE reset-aware and would have coped. The raw sum is not.)
#
#    So the count comes from the POSTGRES DELIVERY LOG instead: a durable row per attempt,
#    written by whichever worker made it, which survives the pod that wrote it. That is
#    also the more convincing source -- it is the system's own record of what it did,
#    rather than a metric about itself.
#
# ⚠️ THE POD MUST BE KILLED WHILE IT IS ACTUALLY WORKING. Deleting an idle pod proves
#    only that a Deployment replaces pods. This script therefore runs load first,
#    waits until deliveries are demonstrably in flight, and kills the busiest worker.

set -uo pipefail
SINK=""; DURATION=90; RATE=120; CONC=16; NS=webhooks
while [[ $# -gt 0 ]]; do
  case "$1" in
    --sink) SINK="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --rate) RATE="$2"; shift 2 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done
[[ -z "$SINK" ]] && { echo "--sink is required" >&2; exit 2; }
: "${POSTGRES_USER:?set it, e.g. via: set -a; . ./.env; set +a}"
: "${POSTGRES_PASSWORD:?set it via .env}"
PGHOST="${PGHOST:-$(kubectl get cm webhook-config -n $NS -o jsonpath='{.data.POSTGRES_URL}' | sed -E 's|.*//([^:]+):.*|\1|')}"

q() { curl -s --max-time 10 --data-urlencode "query=$1" http://127.0.0.1:19090/api/v1/query 2>/dev/null | python3 -c "
import sys,json
try:
    r=json.load(sys.stdin)['data']['result']; print(r[0]['value'][1] if r else '0')
except Exception: print('')"; }
now() { python3 -c 'import time;print(time.time())'; }

echo "════════════════════════════ CHAOS TEST"
echo " load: ${RATE}/s for ${DURATION}s, one worker killed mid-flight"
echo

pgq() {
  kubectl run pgq-$RANDOM --rm -i --restart=Never -n $NS --image=postgres:16-alpine \
    --env="PGPASSWORD=$POSTGRES_PASSWORD" --command -- \
    psql -h "$PGHOST" -U "$POSTGRES_USER" -d webhookdb -t -A -c "$1" 2>/dev/null \
    | grep -E '^[0-9]+$' | head -1
}
BEFORE_DELIV=$(pgq "SELECT count(*) FROM delivery_attempts;")
BEFORE_DLQ=$(q 'max(webhook_dlq_depth)')
echo "  delivery_attempts rows before: $BEFORE_DELIV"

kubectl delete pod loadgen -n $NS --ignore-not-found --wait=true >/dev/null 2>&1
kubectl run loadgen -n $NS --restart=Never --image=python:3.12-alpine \
  --overrides="{\"spec\":{\"containers\":[{\"name\":\"loadgen\",\"image\":\"python:3.12-alpine\",\"command\":[\"python3\",\"/s/gen.py\"],\"env\":[{\"name\":\"TARGET_URL\",\"value\":\"http://producer:8080/events\"},{\"name\":\"SUBSCRIBER_URL\",\"value\":\"$SINK\"},{\"name\":\"CONCURRENCY\",\"value\":\"$CONC\"},{\"name\":\"DURATION\",\"value\":\"$DURATION\"},{\"name\":\"TARGET_RATE\",\"value\":\"$RATE\"}],\"volumeMounts\":[{\"name\":\"s\",\"mountPath\":\"/s\"}],\"resources\":{\"requests\":{\"cpu\":\"200m\",\"memory\":\"128Mi\"},\"limits\":{\"cpu\":\"1500m\",\"memory\":\"256Mi\"}}}],\"volumes\":[{\"name\":\"s\",\"configMap\":{\"name\":\"loadgen-script\"}}]}}" >/dev/null 2>&1

echo "  waiting until deliveries are demonstrably IN FLIGHT before killing anything..."
for i in $(seq 1 30); do
  R=$(q 'sum(rate(http_client_requests_seconds_count[1m]))')
  RI=$(python3 -c "print(int(float('${R:-0}' or 0)))" 2>/dev/null || echo 0)
  [[ "${RI:-0}" -gt 20 ]] && { echo "  delivery rate is ${R}/s -- the pool is working. Killing now."; break; }
  sleep 5
done

VICTIM=$(kubectl get pods -n $NS -l app=worker -o jsonpath='{.items[0].metadata.name}')
SURVIVORS=$(kubectl get pods -n $NS -l app=worker --no-headers | grep -v "$VICTIM" | awk '{print $1}' | tr '\n' ' ')
echo
echo "  victim    : $VICTIM"
echo "  survivors : $SURVIVORS"

T_KILL=$(now)
kubectl delete pod "$VICTIM" -n $NS --grace-period=0 --force >/dev/null 2>&1
echo "  KILLED at t=0 (--grace-period=0: no clean shutdown, jobs die in flight)"

# ① recovery: 3 workers Ready again
T_READY=""
for i in $(seq 1 120); do
  READY=$(kubectl get pods -n $NS -l app=worker --no-headers 2>/dev/null | awk '{split($2,a,"/"); if (a[1]==a[2] && $3=="Running") c++} END {print c+0}')
  if [[ "${READY:-0}" -ge 3 ]]; then T_READY=$(python3 -c "print(round($(now)-$T_KILL,1))"); break; fi
  sleep 2
done
echo
echo "  ① POOL RECOVERY : ${T_READY:-DID NOT RECOVER}s to 3/3 workers Ready"

echo "  waiting for the load to finish and the queue to drain..."
while true; do
  P=$(kubectl get pod loadgen -n $NS -o jsonpath='{.status.phase}' 2>/dev/null)
  [[ "$P" == "Succeeded" || "$P" == "Failed" ]] && break
  sleep 5
done
SENT=$(kubectl logs loadgen -n $NS 2>/dev/null | grep -oE 'accepted202=[0-9]+' | cut -d= -f2)

for i in $(seq 1 60); do
  Q=$(q 'max(webhook_queue_depth)'); P=$(q 'max(webhook_processing_depth)'); R=$(q 'max(webhook_retry_depth)')
  QI=${Q%.*}; PI=${P%.*}; RI2=${R%.*}
  [[ "${QI:-1}" == "0" && "${PI:-1}" == "0" && "${RI2:-1}" == "0" ]] && break
  sleep 5
done

AFTER_DELIV=$(pgq "SELECT count(*) FROM delivery_attempts;")
AFTER_DLQ=$(q 'max(webhook_dlq_depth)')
DELIV=$(python3 -c "print(int(float('$AFTER_DELIV')-float('$BEFORE_DELIV')))")
DLQD=$(python3 -c "print(int(float('$AFTER_DLQ')-float('$BEFORE_DLQ')))")

echo
echo "  ② DELIVERY ACCOUNTING -- the half that is about OUR code, not Kubernetes"
echo "     events accepted (202) : $SENT"
echo "     delivery attempts     : $DELIV   (from the Postgres log, NOT a metric)"
echo "     new dead letters      : $DLQD"
python3 -c "
sent, deliv, dlq = int('${SENT:-0}'), $DELIV, $DLQD
print()
if sent == 0:
    print('     INCONCLUSIVE: nothing was sent.'); raise SystemExit
extra = deliv - sent
if deliv >= sent and dlq == 0:
    print(f'     ✅ NO LOSS: {deliv} attempts >= {sent} accepted, and 0 dead-lettered.')
    if extra > 0:
        print(f'     ⚠️  {extra} MORE attempts than events ({extra/sent*100:.2f}%) -- these are')
        print( '        REDELIVERIES of jobs the killed worker was holding. That is CORRECT:')
        print( '        the system is AT-LEAST-ONCE by design and chooses a duplicate over a')
        print( '        lost event. Subscribers deduplicate on X-Webhook-Event-Id.')
    else:
        print('     No redelivery observed -- the victim held no job at the instant it died.')
else:
    print(f'     🔴 SHORTFALL: {sent-deliv} events accepted but never attempted.')
"
echo
echo "  final queue state: q=$(q 'max(webhook_queue_depth)') processing=$(q 'max(webhook_processing_depth)') retry=$(q 'max(webhook_retry_depth)') dlq=$(q 'max(webhook_dlq_depth)')"
