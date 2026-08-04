# Distributed Webhook Delivery Service

**Reliable at-least-once HTTP webhook delivery** — exponential-backoff retries with jitter, a dead-letter queue for permanent failures, HMAC-SHA256 request signing, and SSRF-hardened ingest.

![Java](https://img.shields.io/badge/Java-21-orange)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3-brightgreen)
![Redis](https://img.shields.io/badge/Redis-7-red)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)

> A self-directed project built to work through distributed-systems fundamentals — delivery
> semantics, reliable queueing, failure classification, and idempotency — as production code
> rather than as reading. **The delivery engine is complete and running locally.**
> Containerization and an AWS EKS deployment are in progress.

---

## The problem

When something happens in your system — a payment succeeds, a build finishes — other systems
need to know. The alternative is *polling*: every interested party asking "anything yet?" every
few seconds, forever.

Webhooks invert it: you call *them*. But that hands you a hard problem, because you are now
making an HTTP request to a server **you do not control**. It can be slow, down, rate-limiting,
or quietly broken — and delivery has to survive all of it without losing events and without
hammering a struggling server.

That's the problem this service solves, and it's the same one Stripe, GitHub, Twilio and Slack
each solve internally.

## What works today

| Capability | Status |
|---|---|
| `POST /events` → validated → queued → `202 Accepted` | ✅ |
| Reliable queue — atomic hand-off, acknowledge only on a confirmed 2xx | ✅ |
| Failure classification → retry vs. dead-letter | ✅ |
| Exponential backoff **with jitter**, scheduled in a Redis sorted set | ✅ |
| Dead-letter queue with a typed reason per entry | ✅ |
| Multiple workers sharing one queue + recovery sweep for crashed workers | ✅ |
| HMAC-SHA256 signing, per attempt, with a stable event id for deduplication | ✅ |
| SSRF validation — every URL judged by the **IP it resolves to** | ✅ |
| Durable delivery-attempt log in PostgreSQL (metadata only, never payloads) | ✅ |
| JUnit 5 + Mockito suite with JaCoCo coverage | 🚧 in progress |
| Docker images + Kubernetes manifests | ○ next |
| AWS EKS deployment, Prometheus + Grafana dashboards | ○ planned |

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

## Design decisions

| Decision | Why |
|---|---|
| **At-least-once, not exactly-once** | Exactly-once is impossible over an unreliable network. Every event carries a stable `event_id` as an idempotency key so duplicates are recognized and discarded — the approach Stripe and GitHub use. |
| **`RPOPLPUSH` + explicit ack, not `BLPOP`** | `BLPOP` deletes the job the instant it hands it out; a worker dying mid-delivery loses an event that was already accepted. `RPOPLPUSH` *moves* it, so the job is never in zero places. |
| **Retries wait in Redis, not in the worker** | `Thread.sleep` blocks one of a small number of workers for up to 16s **and** the delay lives only in process memory, so a restart loses it. A sorted set scored by due-time survives both. |
| **Backoff is jittered** | 1s → 2s → 4s → 8s → 16s, randomized. Without jitter, a thousand deliveries that failed together retry together and take down a server that was recovering. |
| **Sign at send time, never at enqueue** | A signature made at enqueue is minutes old by the time a retried delivery goes out, so the receiver rejects it as stale — and the happy path passes under both designs, which is what makes it easy to get wrong. |
| **Judge the resolved IP, never the hostname** | The attacker registers the hostname. `localtest.me` is a real public domain that resolves to `127.0.0.1`, so a string blocklist catches only honest typos. |
| **Three Maven modules, not two** | The producer serializes a job into Redis and the worker deserializes it — a wire contract between two processes. A shared module makes a schema change a compile error instead of a runtime deserialization failure. |
| **The delivery log stores no payloads** | Payloads may contain PII. The queue needs them transiently; a permanent, queryable, backed-up audit log does not. A column that doesn't exist cannot be written to by a future code path. |

## Reliability, demonstrated

Every claim above was verified by *running* the failure, not by reasoning about it.

- **A worker `kill -9`'d mid-delivery loses nothing.** Three real worker processes, nine events
  against a deliberately slow endpoint so the work actually overlapped — mid-flight state
  `queue=6 processing=3 inflight=3`, split 3/3/3 across disjoint sets. One worker was killed with
  `SIGKILL` while holding a job; the job and its pickup timestamp survived, the survivors kept
  delivering without a gap, and ~60s later a sibling reclaimed and completed the orphan.
  **`RECLAIMED` appears exactly once across all three logs** — two workers swept the same job
  every 10 seconds for a minute and only one claimed it, which is what the Lua script guarantees.
- **A subscriber that accepts the connection and never answers is cut off at 10 seconds** —
  measured at 10,011 ms — instead of pinning a worker indefinitely.
- **The signature is verified by a second implementation.** A ~100-line Python subscriber written
  from the published spec, importing none of the Java, computes a **byte-identical** signature. On
  the retry path the same event produces three distinct timestamps and three distinct signatures
  but one event id — the pair that must disagree, disagreeing.
- **The SSRF guard was disabled on purpose to prove it is what blocks.** With the check removed,
  `http://169.254.169.254/latest/meta-data/` returned `202` and landed on the queue as a real job
  with a retry budget. Restored, the same URL returns `400` **and the queue length does not
  move** — the status code isn't the proof; the absent side effect is.
- **Payloads never appear in application logs or in the delivery-attempt table**, asserted by
  counting a unique marker in both, with a line-count sanity check so a zero can't come from an
  empty file.

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

**Known gaps, named deliberately** — security work is only credible if the holes are stated too:

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

Those two groups aren't decoration — `/actuator/health/liveness` and `/actuator/health/readiness`
are the exact endpoints Kubernetes probes call to decide whether to restart a pod or stop routing
traffic to it.

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

> ⚠️ **A test subscriber on `localhost` will be refused with a `400`, and that's correct.**
> `localhost` is `127.0.0.1`, and the SSRF check cannot distinguish a harmless test server from an
> attacker's internal target — there is no difference visible from the address. Use a public
> endpoint when trying this out. A dev-profile allowlist would solve it and is deliberately not
> implemented: a bypass switch is exactly the thing that reaches production still enabled.

### The delivery log

```sql
SELECT event_id, attempt_number, status_code, success, duration_ms FROM delivery_attempts;
```

```
              event_id              | attempt_number | status_code | success | duration_ms
------------------------------------+----------------+-------------+---------+-------------
 3f2504e0-4f89-11d3-9a0c-0305e82c33 |              1 |         200 | t       |          87
 1111000a-0000-0000-0000-0000000001 |              1 |             | f       |       10024
```

**`status_code` is nullable on purpose.** The second row is a timeout — no response ever arrived,
so there is no status code. Writing `500` there would claim the subscriber reported a failure,
when it may have succeeded and simply answered too late. Different problems, different owners, and
only `NULL` says which happened.

## Tech stack

**In use** — Java 21 · Spring Boot 3.3 · Maven (multi-module) · Spring WebClient · Spring Data
Redis (Lettuce) · Redis 7 incl. sorted sets and Lua scripting · Spring Data JPA / Hibernate ·
PostgreSQL 16 · Flyway · JUnit 5 · Mockito · JaCoCo · Docker Compose

**Planned** — Docker (multi-stage, distroless, non-root) · Kubernetes · Helm · AWS EKS,
ElastiCache, RDS, ALB Ingress, ACM, Secrets Manager · Micrometer · Prometheus · Grafana

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| Foundations | System design, data model, failure modes, security architecture | ✅ Complete |
| Core pipeline | `POST /events` → Redis → worker → HTTP delivery → PostgreSQL log | ✅ Complete |
| Resilience & security | Retry with jitter · DLQ · multi-worker recovery sweep · HMAC signing · SSRF validation | ✅ Complete |
| Test suite | JUnit 5 + Mockito across all three modules, JaCoCo-tracked | 🚧 In progress |
| Containerization | Multi-stage distroless images · Kubernetes manifests · local cluster deploy | ○ Next |
| Cloud deployment | AWS EKS · ElastiCache + RDS in private subnets · ALB Ingress · HTTPS · IRSA | ○ Planned |
| Observability | Prometheus + Grafana dashboards · load testing · documented chaos test | ○ Planned |
