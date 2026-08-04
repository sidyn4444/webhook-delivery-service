# Distributed Webhook Delivery Service

A backend service in Java/Spring Boot that reliably delivers HTTP webhooks to subscriber
URLs across a pool of stateless workers — with exponential-backoff retries, a dead-letter
queue for permanent failures, HMAC-SHA256 request signing, and SSRF-hardened URL validation.
Kubernetes orchestration on AWS EKS and Prometheus/Grafana observability are the remaining
phases.

> **Status: Week 3 of 6 — the delivery engine is feature-complete and running locally.**
>
> **Built and verified by execution:**
> `POST /events` validates, refuses unsafe URLs, enqueues to Redis and returns `202` · workers
> pull jobs with a reliable-queue pattern and acknowledge only on a confirmed 2xx · failures are
> classified and retried with **exponential backoff plus jitter** from a Redis sorted set ·
> permanently-failed and retry-exhausted jobs are **dead-lettered** with a reason · **multiple
> workers share the queue**, and a job held by a worker killed mid-delivery is reclaimed by a
> sibling via an atomic Lua script · every outbound POST is **HMAC-SHA256 signed per attempt**
> and carries a stable event id for receiver-side deduplication · every subscriber URL is judged
> by **the IP it resolves to**, not the hostname it wears · every attempt is a durable row in
> Postgres.
>
> **Not built yet:** Dockerfiles and Kubernetes manifests (Week 4), the AWS EKS deployment
> (Week 5), and Prometheus/Grafana dashboards plus load-test benchmarks (Week 6). The JUnit 5 +
> Mockito suite is in progress. See [Roadmap](#roadmap).

---

## The problem

When something happens in your system — a payment succeeds, a build finishes, a file
uploads — other systems need to know. The alternative to webhooks is *polling*: every
interested party asking "has anything happened yet?" every few seconds, forever. That
wastes work on both sides and still adds latency.

Webhooks invert it: you call *them* when the event occurs. But that hands you a hard
problem, because you are now making an HTTP request to a server **you do not control**.
It can be slow, down, rate-limiting, or quietly broken. Delivery has to survive all of
that without losing events and without hammering a struggling server.

That is the problem this service solves, and it's the same one Stripe, GitHub, Twilio,
and Slack each solve internally.

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

**The delivery cycle.** A worker atomically moves a job from `queue` to `processing` and
records the pickup time in `inflight`. On a confirmed 2xx it writes the attempt row *first*,
then acknowledges. On a retryable failure it schedules the job in `retry` with a jittered
due-time; on a permanent one — or once the retry budget is spent — it goes to `dlq` with a
reason. A separate sweep thread looks for jobs whose pickup time is older than the hard
delivery timeout allows, and re-queues them, which is what makes a worker dying mid-delivery
survivable.

**Producer and worker are separate deployables** so they scale independently: the producer
scales with inbound traffic, the workers with delivery backlog. Redis is the only thing
they share, which is what decouples them — a slow subscriber can never slow down event
ingestion.

### Key design decisions

| Decision | Why |
|---|---|
| **At-least-once delivery, not exactly-once** | Exactly-once is impossible over an unreliable network (the Two Generals problem). Every event carries a stable `event_id` as an idempotency key, so duplicates are recognized and discarded — the same approach Stripe and GitHub use. |
| **`RPOPLPUSH` + ack, not `BLPOP`** | `BLPOP` deletes the job the instant it hands it out; if the worker dies mid-delivery the event is gone forever. `RPOPLPUSH` *moves* it to a processing list, so a crashed worker leaves a trace and a recovery sweep re-queues it. |
| **Exponential backoff with jitter** | 1s → 2s → 4s → 8s → 16s. Fast recovery from brief blips, hard back-off from real outages, and randomized so a thousand simultaneous failures don't retry in synchronized waves against a recovering server. |
| **4xx → immediate DLQ; 5xx/429/timeout → retry** | A 4xx means the request itself is wrong; retrying it is deterministically useless. Only transient failures are worth retrying. |
| **Three Maven modules, not two** | The producer serializes `DeliveryJob` into Redis and the worker deserializes it out — that's a wire contract between two processes. A shared `common` module makes a schema change a compile error at build time instead of a deserialization failure in production. |
| **Delivery log stores metadata, never payloads** | Payloads may contain PII. The queue needs them transiently; a permanent, queryable audit log does not. Data minimization. |
| **Secrets from AWS Secrets Manager via IRSA** *(decided, Week 5 — not yet built)* | Pods assume an IAM role through a Kubernetes service account — no long-lived AWS credentials in code, images, or manifests. Chosen over Kubernetes Secrets for encryption at rest, rotation and audit — and because the secret fetch is the only AWS API call in the system, so Kubernetes Secrets would leave IRSA with no job to do. |

## Reliability, demonstrated

Every claim above was verified by running the failure, not by reasoning about it. The runs
that matter:

- **A worker `kill -9`'d mid-delivery loses nothing.** Three real worker processes, nine
  events against a deliberately slow endpoint so the work actually overlapped — mid-flight
  state `queue=6 processing=3 inflight=3`, work split 3/3/3 across disjoint sets. One worker
  was killed with `SIGKILL` while holding a job; the job and its pickup timestamp survived in
  Redis, the two survivors delivered every subsequent event without a gap, and ~60s later a
  sibling reclaimed and completed the orphan. **`RECLAIMED` appears exactly once across all
  three logs** — two workers swept the same job every 10 seconds for a minute and only one
  claimed it, which is what the Lua script exists to guarantee.
- **A subscriber that accepts the connection and never answers is cut off at 10 seconds** —
  measured at 10,011 ms — rather than pinning a worker indefinitely.
- **The signature is verified by a different implementation.** A ~100-line Python subscriber
  written from the published spec, importing none of the Java, computes a **byte-identical**
  signature. On the retry path the same event produces **three distinct timestamps and three
  distinct signatures but one event id** — the pair that must disagree, disagreeing.
- **The SSRF guard was disabled on purpose to prove it is what blocks.** With the check
  removed, `http://169.254.169.254/latest/meta-data/` returned `202` and landed on the queue
  as a real job with a retry budget. Restored, the same URL returns `400` **and the queue
  length does not move** — the status code is not the proof; the absent side effect is.
  Blocked inputs include `localtest.me` and `*.nip.io`, genuine public domains that resolve
  to `127.0.0.1` and `10.0.0.5` and defeat any hostname-based blocklist.
- **Payloads never appear in our logs or in the delivery-log table** — asserted by counting a
  unique marker in both, with a line-count sanity check so a zero can't come from an empty file.

## Security

- **HMAC-SHA256 signing** on every outbound webhook (`X-Webhook-Signature`), verified by the
  subscriber against a shared secret — proves authenticity and integrity.
- **SSRF prevention** on subscriber URLs: DNS is resolved *first* and the resulting **IP** is
  checked against blocked ranges (loopback, RFC-1918 private, link-local incl. the
  `169.254.169.254` cloud metadata endpoint, IPv6 local). Judging the hostname string
  instead of the resolved address is the mistake that made SSRF a top-10 vulnerability.
- **Hard 10s timeout** on every outbound call, so one unresponsive subscriber cannot occupy a
  worker indefinitely.
- **No secrets in git**: `.env` is gitignored, `.env.example` is committed with dummy values,
  and configuration reads `${ENV_VAR}` placeholders with **no defaults**, so a missing secret
  fails startup instead of silently running unauthenticated.
- **Planned for Week 5** (not yet built): narrow IAM roles for pods via IRSA; ElastiCache and
  RDS in private subnets with security groups admitting only the EKS node group.

### Known limitations, stated deliberately

Security work is only credible if the gaps are named too:

- **DNS rebinding is not defended.** The URL is validated at the front door and re-resolved by
  the worker at delivery time — two moments, and only the first is checked. Pinning the
  validated IP through to the connection is the fix; it is out of scope.
- **Producer-side deduplication is not implemented.** Every event carries a stable `event_id`
  and the outbound request sends it, but the producer does not currently reject a repeated id,
  so 100% of the deduplication burden sits on the receiver.
- **Rejections are logged, not audited.** There is no durable record of blocked URLs, because
  the producer has no database dependency.
- **No rate limiting yet.** A caller can probe the URL validator indefinitely at one `400` each.

## Tech stack

**In use today** — Java 21 · Spring Boot 3.3 · Maven (multi-module) · Spring WebClient ·
Spring Data Redis (Lettuce) · Redis 7 incl. sorted sets and Lua scripting · Spring Data JPA +
Hibernate · PostgreSQL 16 · Flyway · JUnit 5 + Mockito + JaCoCo · Docker Compose

**Planned** — Docker (multi-stage, distroless) · Kubernetes 1.30 · Helm 3 · AWS EKS,
ElastiCache, RDS, ALB Ingress, ACM, Secrets Manager · Micrometer · Prometheus · Grafana

## Repository layout

```
producer/     Spring Boot service — receives events, validates, enqueues
worker/       Spring Boot daemon  — pulls jobs, delivers, retries, dead-letters
common/       Shared library      — models, Redis key constants, HMAC utilities
docker/       Dockerfiles (multi-stage, distroless)          [Week 4]
k8s/local/    Manifests for minikube                          [Week 4]
k8s/eks/      Manifests for the AWS cluster                   [Week 5]
deploy/       eksctl scripts, cluster bootstrapping           [Week 5]
monitoring/   Prometheus + Grafana Helm values, dashboards    [Week 6]
scripts/      Load-testing and chaos-testing                  [Week 6]
```

## Running it locally

**Prerequisites:** Java 21, Maven 3.9+, Docker.

```bash
# 1. Configure the environment
cp .env.example .env          # then edit .env with real local values

# 2. Start the backing services (Redis + Postgres, both password-protected)
docker compose up -d
docker compose ps             # both should report (healthy)

# 3. Build every module
mvn clean install

# 4. Run the producer — serves HTTP on :8080
java -jar producer/target/producer-0.0.1-SNAPSHOT.jar

# 5. Run the worker — a daemon, no web server
java -jar worker/target/worker-0.0.1-SNAPSHOT.jar
```

Verify the producer is up:

```bash
curl localhost:8080/actuator/health
# {"status":"UP","groups":["liveness","readiness"]}
```

Those two groups are not decoration — `/actuator/health/liveness` and
`/actuator/health/readiness` are the exact endpoints Kubernetes probes call to decide
whether to restart a pod or stop routing traffic to it.

### Sending an event end to end

With both processes running, deliver a webhook to any URL that accepts a POST:

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

The `202` is deliberate: the event has been *queued*, not delivered. Returning `200` would
claim it reached the subscriber, and a caller who believes that will never retry.

> ⚠️ **A local test subscriber on `localhost` will be refused with a `400`, and that is correct
> behaviour.** `localhost` resolves to `127.0.0.1`, and the SSRF check cannot distinguish a
> harmless test server from an attacker's internal target — there is no difference visible from
> the address. Use a public endpoint (webhook.site, or any host you control) when trying this
> out. A dev-profile allowlist would solve it and is deliberately not implemented: a bypass
> switch is exactly the thing that reaches production still enabled.

Watch it move through the system:

```bash
# in flight — the job is atomically moved, never deleted, so a crash can't lose it
docker exec webhook-redis redis-cli -a "$REDIS_PASSWORD" --no-auth-warning \
  LLEN webhooks:queue

# the durable record — one row per attempt, success or failure
docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" webhook-postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "SELECT event_id, attempt_number, status_code, success, duration_ms
      FROM delivery_attempts ORDER BY id DESC LIMIT 5;"
```

```
              event_id              | attempt_number | status_code | success | duration_ms
------------------------------------+----------------+-------------+---------+-------------
 3f2504e0-4f89-11d3-9a0c-0305e82c33 |              1 |         200 | t       |          87
 1111000a-0000-0000-0000-0000000001 |              1 |             | f       |       10024
```

**`status_code` is nullable on purpose.** The second row is a timeout — no response ever
arrived, so there is no status code. Writing `500` there would claim the subscriber's server
reported a failure, when in fact it may have succeeded and simply answered too late. Those
are different problems with different owners, and only `NULL` says which one happened.

**Tearing down:** `docker compose down` stops the containers and keeps the data volumes.
Add `-v` to delete the data as well.

## Roadmap

| Week | Scope | Status |
|---|---|---|
| 1 | Distributed-systems fundamentals · system design · security architecture · dev environment · multi-module Maven skeleton | ✅ **Complete** |
| 2 | `POST /events` producer · worker pulling from Redis · delivery log via JPA · end-to-end locally | ✅ **Complete** |
| 3 | Multiple workers · exponential-backoff retry · DLQ · HMAC signing · SSRF validation · JUnit/Mockito suite (80% target) | 🚧 **In progress** — retry, DLQ, recovery sweep, HMAC and SSRF all complete; the test suite is underway |
| 4 | Dockerfiles (multi-stage, distroless, non-root) · Kubernetes manifests · local deploy to minikube | Planned |
| 5 | AWS EKS via eksctl · ElastiCache + RDS in private subnets · ALB Ingress · HTTPS via ACM · IRSA | Planned |
| 6 | Prometheus + Grafana via Helm · dashboards · load testing · documented chaos test · benchmarks | Planned |

---

*Built as a self-directed project to work through distributed-systems fundamentals,
container orchestration, and cloud deployment end to end.*
