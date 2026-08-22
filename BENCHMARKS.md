# Benchmarks

Measured 2026-08-18 on AWS EKS. Cluster was EKS 1.34, 2 × `t3.medium` nodes, ElastiCache
`cache.t3.micro`, RDS `db.t3.micro`, 2 producer pods and 3 worker pods.

## Results

| | |
|---|---|
| Delivery throughput | **178/sec** |
| Delivery latency | **p50 2.5 ms · p95 4.8 ms · p99 5.3 ms** |
| Errors | **0** |

Sustained means the queue wasn't growing. Over 120 seconds the producer took 178.4 events/sec and
the workers delivered 178.2/sec, and the queue absorbed an early burst of 50 and dropped back down
to 12.

That distinction matters because `POST /events` returns `202` as soon as the job is queued — it
doesn't promise anything about delivery. A throughput number without a flat queue is really just
measuring the load generator.

## The ramp

| target | accepted | delivered | queue | p95 | |
|---|---|---|---|---|---|
| 150/s | 150.0/s | 150.0/s | flat at 11 | 4.8 ms | fine, room to spare |
| 180/s | 178.4/s | 178.2/s | 50 → 12 | 4.8 ms | **the number I'm reporting** |
| unlimited, 3 threads | 576/s | 195.9/s | 0 → 46,510 | 4.8 ms | saturated |
| unlimited, 50 threads | 557/s | 203.2/s | 0 → 23,426 | 5.0 ms | saturated |

## Worker capacity

I measured the workers' ceiling four ways — twice under load, and twice by letting them drain a
backlog with nothing new coming in:

| how | result |
|---|---|
| under load, 50 threads | 203.2/s |
| under load, 3 threads | 195.9/s |
| draining a 22k backlog | 185.0/s |
| draining a 32k backlog | ~190/s |

So capacity is around **195/sec**, and the 178/sec I report is about 91% of that.

## The bottleneck is the workers, not the producer

The producer took **576 events/sec** while the workers delivered **196/sec**. That's the most
useful thing the load test found. At 576/sec the producer returned success for every single
request while the queue grew to 46,510 — from the outside the service looked completely healthy.

So the way to scale this is more workers, not more producers. Three workers do about 65/sec each.

## How I tested it

- The load generator runs **inside the cluster** and posts to the producer's ClusterIP. I tried
  running it from my laptop first and it couldn't push hard enough — it topped out at 108
  deliveries/sec while sitting at 89% of its own limit and throwing 125 client-side errors, and
  the service reported no errors at all. If the generator is the thing that's saturated, you're
  measuring the generator.
- **I measured the generator's own ceiling first**, against an endpoint that does nothing. If I'd
  measured it against `/events` I'd have gotten the same number twice under two different names.
- **Every event id is unique.** The producer rejects repeats, so a tool replaying the same body
  would have been measuring the rejection path.
- **Latency is the workers' outbound POST** — an actual delivery — pulled from histogram buckets
  so it adds up correctly across all three worker pods.

## What would make these numbers wrong

- **The subscriber is an nginx in the same cluster**, reached through its own load balancer. It
  isn't a real third-party endpoint. An earlier run against `httpbin.org` gave a p95 of **373 ms**,
  and almost all of that was someone else's server.
- The delivery still crosses a real network hop, so 4.8 ms isn't a loopback number — but a real
  subscriber out on the internet would be slower.
- **The generator skips the load balancer and TLS**, so these numbers don't cover the public HTTPS
  path. That's tested separately.
- **No autoscaling.** Fixed at 2 producers and 3 workers. The HPA never fired.
- **`t3.medium` is burstable.** These runs were minutes long and didn't run out of CPU credits. A
  multi-hour run might.

# Chaos test

I killed one worker pod with `--grace-period=0` while the load test was running, so it died with
no clean shutdown and jobs still in flight.

| | |
|---|---|
| Worker pool back to 3/3 | **34.2 s** |
| Events lost | **0** — 10,800 accepted, 10,800 delivery attempts, 0 dead-lettered |
| Stranded job picked up again | within 60 s |

The 34 seconds is Kubernetes replacing a missing pod, which it does for any deployment. The
zero-loss part is the queue — a job the dead worker had already taken off the queue is invisible
to Kubernetes, and replacing a pod does nothing to put it back.

## Proving the reclaim actually happened

The first run killed a worker during 5 ms deliveries and didn't catch anything in flight. That
still showed nothing was lost, but it never exercised the recovery path. So I ran it again with a
deliberately slow subscriber (3 seconds per delivery) so jobs were definitely being held —
`webhook_processing_depth` read 3 at the moment of the kill.

A worker that survived then logged this:

```
RECLAIMED event 08c7f640-… — no worker completed it within 60s, re-queued for attempt 1 of 5
Recovery sweep re-queued 1 abandoned job(s) from 'webhooks:processing' to 'webhooks:queue'
```

And that event in Postgres:

```
event_id                              | attempt_number | status_code | success | duration_ms
08c7f640-29cd-4b43-bfc7-51ffe75a9019  |              1 |         200 | t       |           2
```

One row. The killed worker took the job and died before writing anything, the sweep put it back,
and another worker delivered it once. No loss and no duplicate.

Across the whole window: 21,612 rows, 21,612 distinct events, 0 failed.

## The counting mistake I made

My first attempt counted deliveries with `sum(http_client_requests_seconds_count)` and got
**minus 68,058**.

A Prometheus counter lives inside the process that owns it. Kill the pod and that series
disappears, and the replacement starts over at zero — so summing across pods breaks as soon as the
set of pods changes, which is exactly what a chaos test does.

The count comes from the Postgres delivery log instead. A row per attempt outlives the pod that
wrote it.

## What I didn't test

- Only one pod, and only a worker. No producer, node, Redis or RDS failure.
- The 60 second reclaim is a setting, not a discovered limit. Lower it and recovery is faster, but
  you risk grabbing a job that was just slow.
- Duplicates are allowed by design — the subscriber is meant to dedupe on `X-Webhook-Event-Id`.
  This run just happened not to produce one.
