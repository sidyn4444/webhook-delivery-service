# Distributed Webhook Delivery Service

**Reliable at-least-once HTTP webhook delivery** — exponential-backoff retries with jitter, a dead-letter queue for permanent failures, HMAC-SHA256 request signing, and SSRF-hardened ingest.

![Java](https://img.shields.io/badge/Java-21-orange)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3-brightgreen)
![Redis](https://img.shields.io/badge/Redis-7-red)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Docker](https://img.shields.io/badge/Docker-distroless-2496ED)
![Kubernetes](https://img.shields.io/badge/Kubernetes-EKS%201.34-326CE5)
![AWS](https://img.shields.io/badge/AWS-ElastiCache%20%C2%B7%20RDS%20%C2%B7%20ALB-FF9900)
![Prometheus](https://img.shields.io/badge/Prometheus-Grafana-E6522C)

> I built this to learn distributed systems properly — delivery guarantees, queueing, retries,
> idempotency — by making the mistakes in code instead of just reading about them. It runs on
> AWS EKS, and the numbers below were measured there.

**Measured on EKS** — 2 × `t3.medium`, 2 producer pods, 3 worker pods:

| | |
|---|---|
| **Sustained delivery throughput** | **178 deliveries/sec** |
| **Delivery latency** | **p50 2.5 ms · p95 4.8 ms · p99 5.3 ms** |
| **Worker-failure recovery** | **34 s to full pool, zero events lost** |

"Sustained" means the queue backlog didn't grow. How I measured all of this, and what would make
each number wrong, is in **[BENCHMARKS.md](BENCHMARKS.md)**.

---

## The problem

When something happens in your system — a payment succeeds, a build finishes — other systems
need to know. The alternative is *polling*: every interested party asking "anything yet?" every
few seconds, forever.

Webhooks invert it: you call *them*. But that hands you a hard problem, because you are now
making an HTTP request to a server **you do not control**. It can be slow, down, rate-limiting,
or quietly broken — and delivery has to survive all of it without losing events and without
hammering a struggling server.

That's what this service handles. It's the same problem Stripe, GitHub, Twilio and Slack all
have to deal with.

## What works today

| Capability | Status |
|---|---|
| `POST /events` → validated → queued → `202 Accepted` | ✅ |
| Reliable queue — atomic hand-off, acknowledge only on a confirmed 2xx | ✅ |
| Exponential backoff **with jitter**, scheduled in a Redis sorted set | ✅ |
| Dead-letter queue with a typed reason per entry | ✅ |
| Multiple workers sharing one queue + recovery sweep for crashed workers | ✅ |
| HMAC-SHA256 signing, per attempt, with a stable event id for deduplication | ✅ |
| SSRF validation — every URL judged by the **IP it resolves to** | ✅ |
| Durable delivery-attempt log in PostgreSQL (metadata only, never payloads) | ✅ |
| JUnit 5 + Mockito suite — **219 tests** across three modules | ✅ |
| **AWS EKS** — private subnets, ElastiCache + RDS, HTTPS at an ALB via ACM | ✅ |
| **Prometheus + Grafana** — throughput, latency quantiles, DLQ depth, pod health | ✅ |

## Architecture

```
                 ┌──────────────┐
  event source ──►   Producer   │  POST /events → validate → SSRF check → enqueue → 202
   (your app)    │ (Spring Boot)│  a URL resolving to a private IP is a 400, never queued
                 └──────┬───────┘
                        │ LPUSH
                 ┌──────▼────────────────────────────────────────────┐
                 │                     Redis                         │
                 │  webhooks:queue      ready to deliver  (list)     │
                 │  webhooks:processing in flight         (list)     │
                 │  webhooks:inflight   pickup times      (zset)     │
                 │  webhooks:retry      scheduled by due-time (zset) │
                 │  webhooks:dlq        gave up, with a reason (list)│
                 └──────┬────────────────────────────────────────────┘
                        │ RPOPLPUSH (atomic move, not delete)
                 ┌──────▼───────┐
                 │   Workers    │  N replicas, stateless. Each runs three threads:
                 │ (Spring Boot)│    poll loop · retry scheduler · recovery sweep
                 └───┬──────┬───┘
                     │      │ HTTPS POST — X-Webhook-Signature, X-Webhook-Event-Id
                     │      └────────────► subscriber URL (untrusted, third-party)
                     │ JPA
              ┌──────▼───────┐
              │   Postgres   │  delivery attempt log (metadata only, never payloads)
              └──────────────┘
```

**The delivery cycle.** A worker atomically moves a job from `queue` to `processing` and records
the pickup time in `inflight`. On a confirmed 2xx it writes the attempt row *first*, then
acknowledges. On a retryable failure it schedules the job in `retry` with a jittered due-time; on
a permanent one — or once the retry budget is spent — it goes to `dlq` with a reason. A separate
sweep thread finds jobs whose pickup time is older than the hard delivery timeout allows and
re-queues them, which is what makes a worker dying mid-delivery survivable.

**Producer and worker are separate deployables** so they scale independently — the producer with
inbound traffic, the workers with delivery backlog. Redis is the only thing they share, which is
what stops a slow subscriber from ever slowing down event ingestion.

**The load test showed which one is actually the constraint:** the producer accepted
**576 events/sec** while three workers delivered **196/sec**. The bottleneck is the worker pool,
so the scaling lever is worker replicas — an autoscaler on the *producer* would not have helped.

### On AWS

```
   internet
      │  HTTPS (ACM certificate, TLS 1.3, HTTP→HTTPS redirect)
      ▼
 ┌──────────────────┐   /actuator → fixed-response 403 at the load balancer
 │  ALB  (public    │   everything else → producer POD IPs directly (target-type: ip)
 │       subnets)   │   subnets discovered by the kubernetes.io/role/elb TAG, not hardcoded
 └────────┬─────────┘
          │
 ┌────────▼──────────────────────────────────────────────────────┐
 │  EKS 1.34 — nodes in PRIVATE subnets, no public IP at all     │
 │                                                                │
 │   producer × 2 ──┐                        ┌── Prometheus       │
 │   worker   × 3 ──┼── scraped per POD IP ──┤   + Grafana        │
 │                  │  (ServiceMonitor +     └── kube-state-metrics
 │                  │   PodMonitor)                                │
 └──────┬───────────┴──────────────┬─────────────────────────────┘
        │ TLS + AUTH               │ TLS
 ┌──────▼────────┐        ┌────────▼────────┐        ┌──────────────┐
 │  ElastiCache  │        │  RDS PostgreSQL │        │ NAT Gateway  │
 │  Redis 7.1    │        │  16.14          │        │ (outbound    │
 │  private      │        │  private        │        │  only)       │
 └───────────────┘        └─────────────────┘        └──────┬───────┘
                                                             │
                                        worker → subscriber ─┘
```

**Both a security group and private subnets are used, and the subnets are the stronger one.** A
security group is a rule you can write wrong; a subnet with no route out isn't a rule at all. The
datastores only accept traffic from the node security group, and they sit in subnets with no route
to the internet. Outbound delivery goes through a NAT Gateway, so a subscriber sees the NAT's
address, never a node's.

**The worker has no Service**, because nothing calls a worker. So Prometheus finds producers with
a `ServiceMonitor` and workers with a `PodMonitor` — adding a Service just so the scraper could
find them seemed like the wrong way round. Both scrape pod IPs, so three workers give three
separate time series instead of one blended average.

## Observability

![Delivery throughput, success/failure split, latency quantiles and queue backlog](assets/dashboard-overview.png)

![Retries, dead-letter queue by reason, and pod health](assets/dashboard-detail.png)

Six panels and four stat tiles, built from PromQL over Micrometer histograms. The three that
matter most:

- **Delivery throughput** peaks around **200/sec** during the saturation runs, then flat-lines
  when the load stops — the workers are idle, not stuck.
- **Delivery latency** sits flat near zero for the load test and then spikes to **3 s**. That
  spike isn't a regression, it's the chaos test's deliberately slow subscriber, picked so jobs
  would actually be in flight when I killed a worker.
- **Queue backlog is the most important panel.** It climbs to **~48,000** when the producer is
  accepting faster than the workers can deliver, and returns to zero when it isn't. `POST /events`
  returns `202` on enqueue, so a saturated system looks perfectly healthy from the front door —
  this line is the only place the truth shows up.

One thing that's easy to get backwards: delivery counts get summed across pods, because each
worker measures its own traffic, but the queue-depth gauges use `max()`, because every pod reports
the same shared Redis state. Summing them showed a 4-job dead-letter queue as **12**, wrong by
exactly the replica count. That one took me a while to spot.

## Design decisions

| Decision | Why |
|---|---|
| **At-least-once, not exactly-once** | Exactly-once is impossible over an unreliable network. Every event carries a stable `event_id` as an idempotency key so duplicates are recognized and discarded — the approach Stripe and GitHub use. |
| **`RPOPLPUSH` + explicit ack, not `BLPOP`** | `BLPOP` deletes the job the instant it hands it out; a worker dying mid-delivery loses an event that was already accepted. `RPOPLPUSH` *moves* it, so the job is never in zero places. |
| **Retries wait in Redis, not in the worker** | `Thread.sleep` blocks one of a small number of workers for up to 16s **and** the delay lives only in process memory, so a restart loses it. A sorted set scored by due-time survives both. |
| **Backoff is jittered** | 1s → 2s → 4s → 8s → 16s, randomized. Without jitter, a thousand deliveries that failed together retry together and take down a server that was recovering. |
| **Sign at send time, never at enqueue** | A signature made at enqueue is minutes old by the time a retried delivery goes out, so the receiver rejects it as stale — and the happy path passes under both designs, which is what makes it easy to get wrong. |
| **Judge the resolved IP, never the hostname** | The attacker registers the hostname. `localtest.me` is a real public domain that resolves to `127.0.0.1`, so a string blocklist catches only honest typos. |
| **The delivery log stores no payloads** | Payloads may contain PII. The queue needs them transiently; a permanent, queryable, backed-up audit log does not. A column that doesn't exist cannot be written to by a future code path. |

## Reliability, demonstrated

I tested each of these by actually causing the failure, instead of reasoning that it should work.

- **A worker `kill -9`'d mid-delivery loses nothing.** Three real worker processes, nine events
  against a deliberately slow endpoint so the work actually overlapped — mid-flight state
  `queue=6 processing=3 inflight=3`. I killed one worker with `SIGKILL` while it held a job; the
  job and its pickup timestamp survived, the other two kept delivering, and ~60s later a sibling
  reclaimed and finished the orphan. `RECLAIMED` appears exactly once across all three logs, even
  though two workers swept the same job every 10 seconds for a minute — that's what the Lua
  script guarantees.
- **The signature is verified by a second implementation.** I wrote a ~100-line Python subscriber
  from the published spec, importing none of the Java, and it computes a byte-identical signature.
  On the retry path the same event gives three different timestamps and three different signatures
  but one event id, which is exactly what should happen.
- **I disabled the SSRF guard on purpose to prove it's what blocks.** With the check removed,
  `http://169.254.169.254/latest/meta-data/` returned `202` and landed on the queue as a real job.
  Restored, the same URL returns `400` **and the queue length does not move** — the `400` on its
  own doesn't prove much; the queue not moving does.

### And again on EKS, under load

I repeated the failure tests on the real cluster with the load generator running:

- **A worker killed with `--grace-period=0` while holding three in-flight jobs lost nothing.**
  `webhook_processing_depth` read **3 at the instant of the kill**. A *surviving* worker's log
  then shows `RECLAIMED event 08c7f640-… — no worker completed it within 60s, re-queued`, and
  that event has **exactly one row** in PostgreSQL. No loss, no duplicate.
- **Over a full chaos run: 10,800 events accepted → 10,800 delivery attempts → 0 dead-lettered.**
- **I split the recovery claim in two, because only half of it is my code's doing.** "The pool
  returned to 3/3 in 34 s" is a statement about Kubernetes — any Deployment replaces a missing
  pod. "No event was lost" is the reliable queue: a job the dead worker had already taken off the
  queue is invisible to Kubernetes, and replacing a pod does nothing to put it back.

## Security

- **HMAC-SHA256 signing** on every outbound webhook (`X-Webhook-Signature: t=…,v1=…`), computed
  over `timestamp.payload` so a captured request can't be replayed indefinitely.
- **SSRF prevention** — DNS is resolved *first* and every resulting IP is checked against loopback,
  RFC-1918 private, CGNAT, link-local (including the `169.254.169.254` cloud-metadata endpoint),
  IPv6 unique-local and multicast ranges.
- **Hard 10s timeout** on every outbound call, covering DNS, connect, TLS and response.
- **No secrets in git** — `.env` is gitignored, `.env.example` ships dummy values, and config reads
  `${ENV_VAR}` placeholders with **no defaults**, so a missing secret fails startup rather than
  silently running unauthenticated.

**Known gaps:**

- **DNS rebinding is undefended.** The URL is checked at ingest and re-resolved by the worker at
  delivery time; only the first moment is guarded. Pinning the validated IP is the fix.
- **Deduplication is receiver-side only.** Events carry a stable id and it's sent on the wire, but
  the producer doesn't yet reject a repeated one.
- **No rate limiting**, so a caller can probe the URL validator indefinitely at one `400` each.

## Running it locally

**Prerequisites:** Java 21, Maven 3.9+, Docker.

```bash
cp .env.example .env          # then fill in local values
docker compose up -d          # Redis + Postgres, both password-protected
mvn clean install             # builds all three modules

java -jar producer/target/producer-0.0.1-SNAPSHOT.jar   # HTTP on :8080
java -jar worker/target/worker-0.0.1-SNAPSHOT.jar       # daemon, no web server
```

```bash
curl localhost:8080/actuator/health
# {"status":"UP","groups":["liveness","readiness"]}
```

Those two groups matter: `/actuator/health/liveness` and `/actuator/health/readiness` are the
endpoints Kubernetes probes call to decide whether to restart a pod or stop sending it traffic.

### Sending an event

```bash
curl -X POST localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{
        "event_id": "3f2504e0-4f89-11d3-9a0c-0305e82c3301",
        "subscriber_url": "https://example.com/hooks/orders",
        "payload": "{\"order_id\":8823,\"status\":\"shipped\"}"
      }'
# HTTP 202 — accepted, not yet delivered
```

The `202` is deliberate: the event has been *queued*, not delivered. Returning `200` would claim
it reached the subscriber, and a caller who believes that will never retry.

> **A test subscriber on `localhost` gets refused with a `400`, and that's correct.** `localhost`
> is `127.0.0.1`, and the SSRF check can't tell a harmless test server from an attacker's internal
> target — they look identical from the address. Use a public endpoint to try this out. I didn't
> add a dev-profile allowlist because a bypass switch is exactly the thing that ships still on.

## Tech stack

**In use** — Java 21 · Spring Boot 3.3 · Maven (multi-module) · Spring WebClient · Spring Data
Redis (Lettuce) · Redis 7 incl. sorted sets and Lua scripting · Spring Data JPA / Hibernate ·
PostgreSQL 16 · Flyway · JUnit 5 · Mockito · JaCoCo · Docker Compose

**Infrastructure** — Docker (multi-stage, distroless, non-root) · Kubernetes 1.34 · Helm ·
AWS EKS · ElastiCache Redis · RDS PostgreSQL · ECR · ALB Ingress Controller · ACM · Route 53 ·
IRSA · Micrometer · Prometheus · Grafana

## What's not done

- Only the load balancer controller uses IRSA. The app pods just read Kubernetes Secrets.
- No alerting. There are dashboards but nothing would page anyone — I never set up Alertmanager.
