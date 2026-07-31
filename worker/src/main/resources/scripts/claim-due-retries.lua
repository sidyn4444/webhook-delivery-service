-- ============================================================================
--  claim-due-retries.lua
--
--  Moves every job whose backoff has expired from the retry set onto the main
--  work queue, and does it as ONE INDIVISIBLE OPERATION.
--
--  WHY THIS IS A SCRIPT AND NOT THREE JAVA CALLS
--
--  Every worker pod runs its own scheduler thread, and they all read the same
--  retry set. Written in Java as "find due jobs, then remove them, then push
--  them", there is no ordering that is correct:
--
--    push then remove  -> all three schedulers see the same due job and all
--                         three push it. Every job delivered three times, on
--                         every pass, under completely normal operation.
--
--    remove then push  -> only one scheduler wins the remove, which fixes the
--                         race -- but a pod killed between the remove and the
--                         push loses the job entirely. Not a duplicate. A loss,
--                         with nothing anywhere to recover it from.
--
--  Redis executes a script to completion with nothing else running in between,
--  because Redis is single-threaded. So inside here there is no race to lose
--  and no gap to crash into. The choice of ordering stops mattering, which is
--  the point -- this does not SOLVE the coordination problem, it removes it.
--
--  The alternatives were a dedicated scheduler pod (a single point of failure,
--  and a gap in retries while Kubernetes restarts it) or leader election among
--  the workers (distributed locks, heartbeats, and two leaders whenever clocks
--  disagree). Both coordinate; this needs no coordination at all.
--
--  KEYS[1]  the retry sorted set, scored by due time   (webhooks:retry)
--  KEYS[2]  the main work queue                        (webhooks:queue)
--  ARGV[1]  now, as epoch milliseconds
--  ARGV[2]  the most jobs to move in a single pass
--
--  Returns the jobs that were moved.
-- ============================================================================

-- Everything scored at or below "now" is due. '-inf' is the lower bound because
-- there is no floor -- a job scheduled long ago and somehow missed is still due,
-- and should come back at the front of this list rather than be skipped.
--
-- 🔴 THE LIMIT IS NOT OPTIONAL. Redis runs this script on its single thread, so
-- for however long it takes, every other client of this Redis -- both workers
-- and the producer's LPUSH on the HTTP request path -- is blocked waiting. If a
-- subscriber has been down for an hour, hundreds of thousands of jobs come due
-- the moment it recovers, and an unbounded script would move all of them in one
-- pass and stall the entire system while doing it. The cap turns that into many
-- short passes instead of one long stop.
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])

for i = 1, #due do
    local job = due[i]

    -- No check on ZREM's return value here, and that is deliberate rather than
    -- an omission. Outside a script it is the whole claim mechanism: whoever
    -- gets 1 owns the job. In here, nothing else can possibly have touched the
    -- set between the ZRANGEBYSCORE above and this line, so the removal cannot
    -- fail to find its target.
    redis.call('ZREM', KEYS[1], job)

    -- LPUSH, matching the producer (8c). The worker takes from the other end, so
    -- a retried job joins the BACK of the FIFO line and is completely
    -- indistinguishable from a newly-submitted one -- the delivery path never
    -- learns that retries exist.
    --
    -- The cost, stated honestly: under a long queue the job waits its turn on
    -- top of the backoff it already served, so the real gap between attempts is
    -- the backoff PLUS the current queue depth. Pushing to the front instead
    -- would honour the schedule more precisely, at the price of letting a
    -- failing subscriber's retries starve first-time deliveries.
    redis.call('LPUSH', KEYS[2], job)
end

return due
