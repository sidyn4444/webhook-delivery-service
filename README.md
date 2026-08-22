# Distributed Webhook Delivery Service

![Java](https://img.shields.io/badge/Java-21-orange)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3-brightgreen)
![Redis](https://img.shields.io/badge/Redis-7-red)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Docker](https://img.shields.io/badge/Docker-distroless-2496ED)
![Kubernetes](https://img.shields.io/badge/Kubernetes-EKS%201.34-326CE5)
![AWS](https://img.shields.io/badge/AWS-ElastiCache%20%C2%B7%20RDS%20%C2%B7%20ALB-FF9900)
![Prometheus](https://img.shields.io/badge/Prometheus-Grafana-E6522C)

Java 21 / Spring Boot 3 service that delivers HTTP webhooks to third-party endpoints. Redis-backed
queue with at-least-once delivery, exponential backoff with jitter, a dead-letter queue,
HMAC-SHA256 request signing, and SSRF checks on every subscriber URL. Deployed on AWS EKS.

Measured on EKS (2 × `t3.medium`, 2 producer pods, 3 worker pods): **178 deliveries/sec** with the
queue backlog flat, **p50 2.5 ms · p95 4.8 ms · p99 5.3 ms**, and **zero events lost** when a
worker pod is killed mid-delivery, with the pool back to 3/3 in 34 s. Method and the four
independent capacity measurements are in [BENCHMARKS.md](BENCHMARKS.md).

## Components

| Module | Responsibility |
|---|---|
| `producer` | Spring Boot web app. `POST /events` validates the payload, resolves and checks the subscriber URL, pushes to Redis, returns `202`. |
| `worker` | Daemon, no web server. Three threads: poll loop, retry scheduler, recovery sweep. Signs and delivers, writes the attempt row, then acks. |
| `common` | Job model shared by both. A schema change becomes a compile error instead of a runtime deserialization failure. |
| `k8s/` | Deployments, Services, ConfigMap, Secrets, HPA, probes. `local/` and `eks/` variants. |
| `deploy/cluster.yaml` | eksctl cluster definition — node groups, private subnets, addons. |
| `monitoring/` | kube-prometheus-stack Helm values and the Grafana dashboard JSON. |
| `scripts/` | In-cluster load generator and the chaos test. |

Redis keys: `webhooks:queue` (ready), `webhooks:processing` (in flight), `webhooks:inflight`
(pickup times, zset), `webhooks:retry` (scheduled by due-time, zset), `webhooks:dlq` (gave up,
with a reason).

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

A worker moves a job from `queue` to `processing` rather than deleting it, so the job is never in
zero places, and records when it picked it up. On a 2xx it writes the attempt row first, then
acks. On a retryable failure it schedules the job in `retry` with a jittered due-time; on a
permanent one, or once retries run out, it goes to `dlq` with a reason. The sweep thread re-queues
jobs held past the delivery timeout, which is what makes a worker dying mid-delivery survivable.

Producer and worker are separate deployments and scale independently. The load test found the
workers are the constraint: the producer accepted **576 events/sec** while three workers delivered
**196/sec**, so the scaling lever is worker replicas.

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

Datastores accept traffic only from the node security group and sit in subnets with no route to
the internet. Outbound delivery leaves through a NAT Gateway, so a subscriber sees the NAT address
and never a node's.

Workers have no Service, because nothing calls a worker. Prometheus finds producers with a
`ServiceMonitor` and workers with a `PodMonitor`, both scraping pod IPs, so three workers produce
three separate time series instead of one blended average.

## Setup

Java 21, Maven 3.9+, Docker.

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

`/actuator/health/liveness` and `/actuator/health/readiness` are the endpoints the Kubernetes
probes call to decide whether to restart a pod or stop sending it traffic.

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

`202` means queued, not delivered. A `200` would claim it reached the subscriber, and a caller who
believes that never retries.

A subscriber on `localhost` is refused with a `400`. `localhost` is `127.0.0.1` and the SSRF check
cannot tell a test server from an attacker's internal target — they are the same address. Use a
public endpoint. There is no dev-profile bypass, deliberately.

## Observability

![Delivery throughput, success/failure split, latency percentiles and queue backlog](assets/dashboard-overview.png)

![Retries, dead-letter queue by reason, and pod health](assets/dashboard-detail.png)

Six panels and four stat tiles, PromQL over Micrometer metrics. Queue backlog is the panel that
matters: it climbs to ~48,000 when the producer accepts faster than the workers deliver, and
returns to zero when it doesn't. `POST /events` returns `202` on enqueue, so a saturated system
still looks healthy from the front door.

Delivery counts are summed across pods, because each worker counts its own traffic. Queue-depth
gauges use `max()`, because every pod reports the same shared Redis state — summing them reported
a 4-job dead-letter queue as 12, wrong by exactly the replica count.

## Design notes

- **At-least-once, not exactly-once.** Every event carries a stable `event_id` so the receiver can
  spot and drop duplicates. Same approach Stripe and GitHub use.
- **`RPOPLPUSH` + explicit ack, not `BLPOP`.** `BLPOP` deletes the job the moment it hands it out,
  so a worker dying mid-delivery loses an event that was already accepted.
- **Retries wait in Redis, not in the worker.** `Thread.sleep` ties up one of a small number of
  workers for up to 16s, and the delay only exists in memory, so a restart loses it. A sorted set
  scored by due-time survives both.
- **Signing happens at send time, not at enqueue.** A signature made at enqueue is minutes old by
  the time a retry goes out and the receiver rejects it as stale. Both versions pass the happy
  path, which is why it is easy to get wrong.
- **URLs are judged by the resolved IP, not the hostname.** `localtest.me` is a real public domain
  that resolves to `127.0.0.1`, so blocking strings only catches typos.
- **The delivery log stores no payloads.** Payloads can contain personal data. The queue needs
  them briefly; a permanent log does not.

## Reliability

Each of these was verified by causing the failure, not by reasoning about it.

- **Worker `kill -9`'d mid-delivery, locally.** Three worker processes, nine events against a
  deliberately slow endpoint so the work overlapped. One worker killed while holding a job; the
  job and its pickup time survived, the other two kept delivering, and ~60s later a sibling
  reclaimed and finished the orphan. `RECLAIMED` appears exactly once across all three logs even
  though two workers swept the same job every 10 seconds — the Lua script guarantees that.
- **Worker pod killed with `--grace-period=0` on EKS, under load.** `webhook_processing_depth`
  read 3 at the instant of the kill. A surviving worker logged
  `RECLAIMED event 08c7f640-… — no worker completed it within 60s, re-queued`, and that event has
  exactly one row in PostgreSQL. Over the full run: **10,800 accepted → 10,800 delivery attempts →
  0 dead-lettered**.
- **SSRF check disabled on purpose.** Without it,
  `http://169.254.169.254/latest/meta-data/` returned `202` and landed on the queue as a real job.
  Restored, the same URL returns `400` and the queue length does not move.

The 34 s recovery is Kubernetes replacing a missing pod — any Deployment does that. The zero-loss
result is the queue: a job the dead worker had already taken off the queue is invisible to
Kubernetes.

## Security

- **HMAC-SHA256 signing** on every outbound webhook (`X-Webhook-Signature: t=…,v1=…`), computed
  over `timestamp.payload` so a captured request cannot be replayed indefinitely.
- **SSRF checks** — DNS resolved first, every resulting IP checked against loopback, private,
  link-local (including the `169.254.169.254` metadata address) and internal IPv6 ranges.
- **Hard 10s timeout** on every outbound call, covering DNS, connect, TLS and response.
- **No secrets in git** — `.env` is gitignored, `.env.example` ships dummy values, and config reads
  `${ENV_VAR}` with no defaults, so a missing secret fails startup instead of running
  unauthenticated.

Not covered: DNS rebinding (the URL is checked at ingest and re-resolved at delivery, only the
first is guarded), producer-side deduplication, and rate limiting.

## Stack

Java 21 · Spring Boot 3.3 · Maven (multi-module) · Spring WebClient · Spring Data Redis (Lettuce) ·
Redis 7 with sorted sets and Lua · Spring Data JPA / Hibernate · PostgreSQL 16 · Flyway · JUnit 5 ·
Mockito · JaCoCo · Docker Compose

Docker (multi-stage, distroless, non-root) · Kubernetes 1.34 · Helm · AWS EKS · ElastiCache Redis ·
RDS PostgreSQL · ECR · ALB Ingress Controller · ACM · Route 53 · IRSA · Micrometer · Prometheus ·
Grafana

## Not implemented

Only the AWS Load Balancer Controller uses IRSA; the application pods read Kubernetes Secrets.
No alerting — Alertmanager is off, so the stack is dashboards only.
