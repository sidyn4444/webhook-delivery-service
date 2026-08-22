# Distributed Webhook Delivery Service

![Java](https://img.shields.io/badge/Java-21-orange)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3-brightgreen)
![Redis](https://img.shields.io/badge/Redis-7-red)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Docker](https://img.shields.io/badge/Docker-distroless-2496ED)
![Kubernetes](https://img.shields.io/badge/Kubernetes-EKS%201.34-326CE5)
![AWS](https://img.shields.io/badge/AWS-ElastiCache%20%C2%B7%20RDS%20%C2%B7%20ALB-FF9900)
![Prometheus](https://img.shields.io/badge/Prometheus-Grafana-E6522C)

Sends webhooks — HTTP callbacks — to other people's servers and keeps trying until they go
through. Java 21 and Spring Boot 3, running on AWS EKS.

The hard part is that you don't control the server you're calling. It can be slow, down, or just
broken, and you still can't lose the event. So events go into a Redis queue, workers pull them off
and deliver them, and anything that fails gets retried on a schedule instead of being dropped.

Load test and chaos test results are in [BENCHMARKS.md](BENCHMARKS.md).

## Components

| Module | What it does |
|---|---|
| `producer` | The web app. `POST /events` checks the request, checks the URL is safe to call, puts the job in Redis, and returns `202`. |
| `worker` | Background service, no web server. Three threads: one pulls jobs off the queue and delivers them, one handles retries, one picks up jobs from workers that died. |
| `common` | The job format both sides share, so changing it breaks the build instead of breaking later at runtime. |
| `k8s/` | Kubernetes config — deployments, services, secrets, health checks. Separate versions for local and EKS. |
| `deploy/cluster.yaml` | The EKS cluster definition, used by `eksctl`. |
| `monitoring/` | Prometheus and Grafana setup, plus the dashboard. |
| `scripts/` | The load test and the chaos test. |

The five Redis keys:

- `webhooks:queue` — waiting to be delivered
- `webhooks:processing` — being delivered right now
- `webhooks:inflight` — when each job was picked up
- `webhooks:retry` — scheduled to try again later
- `webhooks:dlq` — gave up, with a reason attached

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
                 │  webhooks:inflight   pickup times (sorted set)    │
                 │  webhooks:retry      due-time order (sorted set)  │
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

A worker **moves** a job from `queue` to `processing` instead of deleting it, so the job is never
in nowhere, and writes down when it picked it up.

If the delivery works, it saves the log row first and then drops the job. If it fails but is worth
retrying, the job goes into `retry` with a time to try again. If it fails for good, or runs out of
retries, it goes to `dlq`.

The third thread looks for jobs that have been sitting in `processing` for too long. That means
the worker holding it died, so it puts them back on the queue. That's the part that makes losing
a worker mid-delivery survivable.

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

Redis and Postgres only accept connections from the cluster's own machines, and they sit on a
private network with no route out to the internet. Outgoing webhooks leave through a NAT Gateway —
a single shared exit — so the subscriber sees that address instead of one of our servers.

Workers don't get a Kubernetes Service, because nothing ever calls a worker. Prometheus scrapes
each worker pod directly instead, so all three report their own numbers rather than getting
averaged into one.

## Design notes

- **At-least-once, not exactly-once.** Exactly-once isn't really possible over a network that can
  drop things. Every event has an id that stays the same across retries, so the receiver can spot
  a duplicate and ignore it. Stripe and GitHub do the same thing.
- **`RPOPLPUSH` instead of `BLPOP`.** `BLPOP` deletes the job the moment it hands it out, so if
  that worker dies you've lost an event you already told the caller you'd accepted. `RPOPLPUSH`
  moves it instead, so it's always somewhere.
- **Retries wait in Redis, not in the worker.** Sleeping inside the worker ties it up for up to 16
  seconds doing nothing, and if it restarts the retry is gone. Storing it in Redis with a time
  attached survives both.
- **Signing happens right before sending, not when the job is queued.** A signature made at queue
  time is minutes old by the time a retry actually goes out, and the receiver rejects it for being
  too old. Both versions work on the first try, which is what makes it easy to get wrong.
- **URLs get checked by the IP they resolve to, not the text.** `localtest.me` is a real domain
  that points at `127.0.0.1`, so blocking suspicious-looking strings only catches honest typos.
- **The delivery log doesn't store payloads.** They can contain personal data. The queue needs them
  for a few seconds; a permanent log doesn't need them at all.

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

Those two groups are what Kubernetes checks to decide whether to restart a pod or stop sending it
traffic.

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

`202` means queued, not delivered. A `200` would be claiming it already reached the subscriber,
and a caller who believes that will never retry.

A subscriber on `localhost` gets refused with a `400`. `localhost` is `127.0.0.1`, and the URL
check can't tell a harmless test server from an attacker pointing us at something internal — they
look the same from the address. Use a public endpoint to try it out.

## Monitoring

Both services expose metrics that Prometheus scrapes, and the dashboard in `monitoring/` shows
delivery rate, successes vs failures, how long deliveries take, how big the queue is, what's in
the dead-letter queue, and pod health.

![Delivery rate, successes vs failures, delivery times and queue size](assets/dashboard-overview.png)

![Retries, dead-letter queue by reason, and pod health](assets/dashboard-detail.png)

The queue size panel is the one that matters most. `POST /events` returns `202` as soon as the job
is queued, so if the workers fall behind the service still looks completely healthy from the
outside — the queue is the only place you'd notice.

## Reliability

I tested these by actually breaking things instead of assuming they'd work.

- **Killed a worker mid-delivery, locally.** Three workers, nine events going to a deliberately
  slow endpoint so deliveries were still in progress. I killed one while it was holding a job. The
  job survived, the other two kept delivering, and about a minute later one of them picked up the
  abandoned job and finished it — exactly once, even though both were checking every 10 seconds.
  The reclaim is a Lua script, which Redis runs start to finish without letting anything else in,
  so only one worker can win it.
- **Killed a worker pod on EKS while it was under load.** Same result. A surviving worker logged
  that it re-queued the abandoned event, and that event ended up with exactly one row in the
  database — not zero, not two. Across the whole run, 10,800 events came in and 10,800 delivery
  attempts went out, with none dead-lettered.
- **Turned the URL check off on purpose** to make sure it was really the thing doing the blocking.
  With it off, a URL pointing at the AWS internal metadata address was accepted and queued as a
  real job. With it back on, the same URL gets a `400` and nothing is added to the queue.

Worth being clear about one thing: the pool coming back in 34 seconds is just Kubernetes replacing
a pod, which it does for any deployment. The part my code is responsible for is that no event was
lost, because Kubernetes has no idea a job was in progress.

## Security

- **Every webhook is signed** with HMAC-SHA256 over the timestamp and the body together, so the
  receiver can check it came from us and that it isn't an old request being replayed.
- **Every URL is resolved first, then checked.** The IP it points at can't be loopback, private,
  link-local (which includes the AWS metadata address), or an internal IPv6 range.
- **Every outgoing call has a hard 10 second timeout**, covering DNS, connecting, TLS and the
  response.
- **No secrets in the repo.** `.env` is gitignored, `.env.example` has fake values, and the config
  has no fallback defaults, so a missing secret crashes the app at startup instead of quietly
  running without auth.

Not handled: DNS rebinding (the URL is checked when it arrives and looked up again at delivery, and
only the first one is guarded), duplicate detection on our side, and rate limiting.

## Stack

Java 21 · Spring Boot 3.3 · Maven · Spring WebClient · Redis 7 (Lettuce, sorted sets, Lua) ·
JPA / Hibernate · PostgreSQL 16 · Flyway · JUnit 5 · Mockito · JaCoCo · Docker Compose

Docker · Kubernetes 1.34 · Helm · AWS EKS · ElastiCache Redis · RDS PostgreSQL · ECR · ALB · ACM ·
Route 53 · Micrometer · Prometheus · Grafana

## What I'd add in v2

- **Stop taking jobs when Postgres is down.** Right now the worker delivers the webhook, fails to
  write the log row, and correctly doesn't ack — so the job goes back on the queue and gets
  delivered again. For as long as the outage lasts the subscriber keeps getting the same webhook.
  The fix is to pause the poll loop while the database is unreachable and pick it back up when it
  recovers.
- **Move the credentials to AWS Secrets Manager.** Only the AWS load balancer controller uses IRSA
  right now — the app pods read their credentials from Kubernetes Secrets, which is base64 in
  etcd, not encryption. Secrets Manager would get encryption at rest, rotation without a redeploy,
  and a record of every read. The IRSA setup already exists for the load balancer controller, so
  this would be a second use of something already working rather than new ground.
- **Alerting.** The dashboards exist but nothing would page anyone. Alertmanager rules on the
  counters already being exported would cover it.
