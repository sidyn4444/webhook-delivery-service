# Benchmarks — measured 2026-08-18 on AWS EKS

**Cluster:** EKS 1.34 · 2 × `t3.medium` (3292Mi allocatable each, measured) · ElastiCache `cache.t3.micro` · RDS `db.t3.micro` · 2 producer pods, **3 worker pods**.

---

## Headline

| | |
|---|---|
| **Sustained delivery throughput** | **178 deliveries/sec** |
| **Delivery latency p95** | **4.8 ms** (p50 2.5 ms · p99 5.3 ms) |
| **Errors during the sustained run** | **0** |

**"Sustained" means the queue backlog did not grow.** Over 120 s at 178.4 accepts/sec the system delivered 178.2/sec — input equal to output — with the backlog absorbing an initial burst of 50 and *declining* to 12.

---

## The ramp

| target in | accepted | delivered | backlog | p95 | verdict |
|---|---|---|---|---|---|
| 150/s | 150.0/s | **150.0/s** | flat at 11 | 4.8 ms | sustained, headroom |
| 180/s | 178.4/s | **178.2/s** | 50 → 12, declining | 4.8 ms | **sustained — the reported N** |
| unlimited (3 threads) | 576/s | 195.9/s | 0 → 46,510, growing | 4.8 ms | saturated |
| unlimited (50 threads) | 557/s | 203.2/s | 0 → 23,426, growing | 5.0 ms | saturated |

## Capacity, measured four independent ways

The worker pool's ceiling was measured both **under load** and by **draining a backlog with zero new input** — two methods that share no mechanism:

| method | result |
|---|---|
| under load, 50 threads | 203.2/s |
| under load, 3 threads | 195.9/s |
| pure drain of a 22k backlog | 185.0/s |
| pure drain of a 32k backlog | ~190/s |

**Capacity ≈ 195/s.** The reported sustained figure of 178/s is 91% of it.

---

## 🔴 The bottleneck is the worker pool, not ingest

The producer accepted **576 events/sec** while the workers delivered **196/s**. That gap is the most useful thing the load test found:

- `POST /events` returns `202` on enqueue and promises nothing about delivery, so **a saturated system looks perfectly healthy from the front door.** At 576/s the producer reported success for every single request while the backlog grew to 46,510.
- The only signal that anything was wrong is `webhook_queue_depth`. **A throughput number quoted without a flat backlog is a measurement of the load generator.**
- Scaling the fix is therefore worker replicas, not producer replicas — 3 workers × ~65/s each.

## Method, and what would invalidate it

- **Load generated from inside the cluster**, posting to the producer's ClusterIP. A laptop generator was tried first and **could not saturate the system**: it peaked at 108.7 deliveries/sec while at 89% of its own measured 122/s ceiling and throwing 125 client-side errors, with the service reporting no 4xx and no 5xx. *A capacity number produced by a saturated generator measures the generator.*
- **The generator's own ceiling was measured first**, against a do-nothing endpoint. Measuring it against `/events` would have made "generator ceiling" and "service ceiling" the same number under two names.
- **Every `event_id` is unique.** The producer enforces idempotency, so a tool replaying one body would measure the duplicate-rejection path and report it as success.
- **Latency is `http_client_requests_seconds`** — the worker's outbound POST, i.e. an actual delivery — computed as `histogram_quantile(0.95, sum(rate(..._bucket[3m])) by (le))` from histogram buckets, so it aggregates correctly across all 3 worker pods.

### ⚠️ Boundaries, stated

- **The subscriber is an nginx in the same cluster, reached through its own public ALB** (worker → NAT → ALB → pod). It is not a third-party endpoint: an earlier run against `httpbin.org` measured **p95 373 ms**, almost all of it someone else's server and the public internet. That number would have been false as a claim about this system.
- **The delivery leg still crosses a real network round trip**, so 4.8 ms is not a loopback figure — but it is faster than a real subscriber across the internet would be.
- **The in-cluster generator bypasses the ALB and TLS**, so these numbers do not exercise the public HTTPS path. That path is proven separately (verified certificate, HTTP→HTTPS redirect, `/actuator` denied) but is not what a throughput number measures.
- **No autoscaling was involved.** Fixed at 2 producers and 3 workers; the HPA was not triggered and is not part of these numbers.
- **`t3.medium` is burstable.** These runs are minutes long and did not exhaust CPU credits; a multi-hour run at this rate might.

---

# Chaos test — `kubectl delete pod` mid-delivery

**Method:** sustained load, then one worker killed with `--grace-period=0` (SIGKILL, no clean shutdown, jobs die in flight).

## 🔴 Two separate claims, and only one of them is about this system

| | measured |
|---|---|
| **① Worker pool back to 3/3 Ready** | **34.2 s** |
| **② Events lost** | **0** — 10,800 accepted → 10,800 delivery attempts → 0 dead-lettered |
| **③ A job stranded mid-delivery, reclaimed and redelivered** | **within 60 s** (the sweep's idle threshold, by design) |

**① is a statement about Kubernetes.** A Deployment notices a missing pod and replaces it; any Deployment does that. **② and ③ are statements about this system's reliable-queue implementation** — a job the dead worker had already taken off the queue is invisible to Kubernetes, and nothing about replacing a pod puts it back.

## The reclaim, proven directly rather than inferred

The first chaos run killed a worker during 5 ms deliveries and caught nothing in flight — honest, and it proved zero loss without exercising the recovery path. A second run used a deliberately slow subscriber (3 s per delivery) so jobs were demonstrably held: **`webhook_processing_depth` read 3 at the instant of the kill.**

A **surviving** worker's log then shows the reclaim:

```
RECLAIMED event 08c7f640-… — no worker completed it within 60s, re-queued for attempt 1 of 5
Recovery sweep re-queued 1 abandoned job(s) from 'webhooks:processing' to 'webhooks:queue'
```

And that event's row in Postgres:

```
event_id                              | attempt_number | status_code | success | duration_ms
08c7f640-29cd-4b43-bfc7-51ffe75a9019  |              1 |         200 | t       |           2
```

**Exactly one row.** The killed worker took the job and died before recording anything; the sweep reclaimed it; another worker delivered it once. **No loss and no duplicate** — 12 events in, 12 rows out.

Across the whole test window: **21,612 rows / 21,612 distinct events / max_attempt 1 / 0 failed.**

## ⚠️ How the accounting was almost wrong

The first version counted deliveries with `sum(http_client_requests_seconds_count)` and reported **minus 68,058**.

🔴 **A Prometheus counter lives in the process that owns it. Kill the pod and the series disappears; the replacement starts a new series at zero.** So `sum(counter)` across pods is *not* monotonic when the pod set changes — and a chaos test is precisely the thing that changes it. (`rate()` and `increase()` are reset-aware and would have coped; the raw sum is not.)

The count therefore comes from the **Postgres delivery log** — a durable row per attempt that outlives the pod that wrote it, and the system's own record of what it did rather than a metric about itself.

### Boundaries

- **Only one pod was killed**, and only a worker. No producer, node, Redis or RDS failure was tested.
- **The 60 s reclaim is a configured threshold, not a discovered limit.** Lowering it shortens recovery and raises the risk of reclaiming a job that was merely slow.
- **At-least-once is by design.** A duplicate is correct behaviour, absorbed by the subscriber deduplicating on `X-Webhook-Event-Id`. This run happened not to produce one.
