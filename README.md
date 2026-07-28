# Distributed Webhook Delivery Service

A backend service in Java/Spring Boot that reliably delivers HTTP webhooks to subscriber
URLs across a Kubernetes-orchestrated worker pool on AWS EKS — with exponential-backoff
retries, a dead-letter queue for permanent failures, HMAC-SHA256 request signing, and
Prometheus/Grafana observability.

> **Status: Week 2 of 6 — the pipe works end to end, locally.** `POST /events` validates and
> enqueues to Redis and returns `202`; a worker pulls jobs with a reliable-queue pattern,
> delivers them over HTTP with a hard 10s timeout, acknowledges only on a confirmed 2xx, and
> records every attempt as a durable row in Postgres.
>
> **Not built yet:** retries, the dead-letter queue, HMAC signing, and SSRF validation — all
> Week 3. A failed delivery currently parks in the processing list and stays there. See
> [Roadmap](#roadmap) for what exists today versus what's coming.

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
  event source ──►   Producer   │  POST /events → validate → enqueue → 202 Accepted
   (your app)    │ (Spring Boot)│
                 └──────┬───────┘
                        │ LPUSH
                 ┌──────▼───────┐
                 │    Redis     │  queue · processing (in-flight) · DLQ
                 └──────┬───────┘
                        │ RPOPLPUSH (atomic move, not delete)
                 ┌──────▼───────┐
                 │   Workers    │  N replicas — HMAC-sign → POST → retry → ack
                 │ (Spring Boot)│
                 └───┬──────┬───┘
                     │      │ HTTPS POST (X-Webhook-Signature)
                     │      └────────────► subscriber URL (untrusted, third-party)
                     │ JPA
              ┌──────▼───────┐
              │   Postgres   │  delivery attempt log (metadata only, never payloads)
              └──────────────┘
```

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
| **Secrets from AWS Secrets Manager via IRSA** | Pods assume an IAM role through a Kubernetes service account — no long-lived AWS credentials in code, images, or manifests. |

## Security

- **HMAC-SHA256 signing** on every outbound webhook (`X-Webhook-Signature`), verified by the
  subscriber against a shared secret — proves authenticity and integrity.
- **SSRF prevention** on subscriber URLs: DNS is resolved *first* and the resulting **IP** is
  checked against blocked ranges (loopback, RFC-1918 private, link-local incl. the
  `169.254.169.254` cloud metadata endpoint, IPv6 local). Judging the hostname string
  instead of the resolved address is the mistake that made SSRF a top-10 vulnerability.
- **Hard 10s timeout** on every outbound call, so one unresponsive subscriber cannot occupy a
  worker indefinitely.
- **Least privilege**: narrow IAM roles for pods via IRSA; ElastiCache and RDS in private
  subnets with security groups admitting only the EKS node group.
- **No secrets in git**: `.env` is gitignored, `.env.example` is committed with dummy values,
  and configuration reads `${ENV_VAR}` placeholders.

## Tech stack

**Application** — Java 21 · Spring Boot 3.3 · Maven (multi-module) · Spring WebClient ·
Spring Data Redis (Lettuce) · Spring Data JPA + Hibernate · JUnit 5 + Mockito
**Infrastructure** — Redis 7 · PostgreSQL 16 · Docker · Kubernetes 1.30 · Helm 3 ·
AWS EKS, ElastiCache, RDS, ALB Ingress, ACM, Secrets Manager
**Observability** — Micrometer · Prometheus · Grafana

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
| 3 | Multiple workers · exponential-backoff retry · DLQ · HMAC signing · SSRF validation · JUnit/Mockito suite (80% target) | **Next** |
| 4 | Dockerfiles (multi-stage, distroless, non-root) · Kubernetes manifests · local deploy to minikube | Planned |
| 5 | AWS EKS via eksctl · ElastiCache + RDS in private subnets · ALB Ingress · HTTPS via ACM · IRSA | Planned |
| 6 | Prometheus + Grafana via Helm · dashboards · load testing · documented chaos test · benchmarks | Planned |

---

*Built as a self-directed project to work through distributed-systems fundamentals,
container orchestration, and cloud deployment end to end.*
