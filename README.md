# Distributed Webhook Delivery Service

A backend service in Java/Spring Boot that reliably delivers HTTP webhooks to subscriber
URLs across a Kubernetes-orchestrated worker pool on AWS EKS — with exponential-backoff
retries, a dead-letter queue for permanent failures, HMAC-SHA256 request signing, and
Prometheus/Grafana observability.

> **Status: Week 1 of 6 — foundations.** Design, security architecture, dev environment,
> and the multi-module Maven skeleton are complete and building. The producer boots and
> serves `/actuator/health`; the worker boots as a daemon. Delivery logic lands in Week 2.
> See [Roadmap](#roadmap) for what exists today versus what's coming.

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

**Tearing down:** `docker compose down` stops the containers and keeps the data volumes.
Add `-v` to delete the data as well.

## Roadmap

| Week | Scope | Status |
|---|---|---|
| 1 | Distributed-systems fundamentals · system design · security architecture · dev environment · multi-module Maven skeleton | **In progress** |
| 2 | `POST /events` producer · worker pulling from Redis · delivery log via JPA · end-to-end locally | Planned |
| 3 | Multiple workers · exponential-backoff retry · DLQ · HMAC signing · SSRF validation · JUnit/Mockito suite (80% target) | Planned |
| 4 | Dockerfiles (multi-stage, distroless, non-root) · Kubernetes manifests · local deploy to minikube | Planned |
| 5 | AWS EKS via eksctl · ElastiCache + RDS in private subnets · ALB Ingress · HTTPS via ACM · IRSA | Planned |
| 6 | Prometheus + Grafana via Helm · dashboards · load testing · documented chaos test · benchmarks | Planned |

---

*Built as a self-directed project to work through distributed-systems fundamentals,
container orchestration, and cloud deployment end to end. 
