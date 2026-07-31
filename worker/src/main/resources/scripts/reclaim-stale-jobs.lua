-- ============================================================================
--  reclaim-stale-jobs.lua
--
--  The recovery sweep, as ONE INDIVISIBLE OPERATION. Two phases:
--
--    1. ADOPT   every job in the processing list that has no pickup time gets
--               one, set to now. It then has to age like everything else.
--    2. RECLAIM every pickup time older than the staleness cutoff means the
--               worker holding that job is dead: take the job out of the
--               processing list, put it back on the main queue, and drop the
--               index entry.
--
--  WHY THIS IS A SCRIPT AND NOT SEVERAL JAVA CALLS
--
--  Every worker pod runs its own sweep thread over the same two keys. Written
--  in Java as "read the stale ones, then move them", there is no ordering that
--  is correct -- the same argument as claim-due-retries.lua (12d):
--
--    push then remove  -> all three sweeps read the same stale job and all
--                         three re-queue it. Three duplicate deliveries per
--                         pass, caused by nothing being wrong.
--
--    remove then push  -> only one sweep wins the remove, which fixes the
--                         race -- and a pod killed in the gap loses the job
--                         outright, which is the one failure this whole
--                         subsystem exists to prevent.
--
--  Redis is single-threaded and runs a script to completion with nothing
--  interleaved, so inside here there is no race to lose and no gap to crash
--  into. The general rule: A BACKGROUND THREAD THAT RUNS IN EVERY WORKER IS N
--  COPIES OF ITSELF OVER SHARED STATE. Anywhere it READS something and then
--  ACTS on what it read must be one script. A single command needs nothing --
--  it is the read-then-act sequence that needs protecting.
--
--  KEYS[1]  the processing list -- the SOURCE OF TRUTH   (webhooks:processing)
--  KEYS[2]  the pickup-time index, a sorted set          (webhooks:inflight)
--  KEYS[3]  the main work queue                          (webhooks:queue)
--  ARGV[1]  now, as epoch milliseconds
--  ARGV[2]  the staleness cutoff, as epoch milliseconds (now - stale-after);
--           anything stamped at or before this is abandoned
--  ARGV[3]  the most jobs to touch in one pass, per phase
--
--  Returns { adopted_count, pruned_count, { the jobs that were re-queued } }
-- ============================================================================

local limit = tonumber(ARGV[3])

-- ---------------------------------------------------------------------------
-- PHASE 1 -- ADOPT THE ORPHANS
--
-- An orphan is a job in the processing list with no entry in the index. It is
-- created by a crash between the two commands that make up a pickup: the
-- blocking RPOPLPUSH that moves the job, and the ZADD that stamps it. Those
-- cannot be merged, because a blocking command does not block inside a script
-- -- it would freeze the whole server -- so the gap is permanent (14a).
--
-- 🔴 ADOPT, DO NOT JUDGE. The temptation is to treat a missing timestamp as
-- suspicious and re-queue immediately. That is wrong, and the reason is that a
-- perfectly healthy worker one microsecond past its pop produces the IDENTICAL
-- state. Re-queueing on that reading would emit a steady drip of duplicate
-- deliveries caused by nothing being wrong at all.
--
-- Stamping it with 'now' costs one staleness window of delay in the genuinely
-- crashed case, and costs nothing in the healthy case -- because the worker's
-- own ZADD, a moment later, simply overwrites the score. Sorted-set members are
-- unique, so there is no duplicate entry to clean up afterwards.
-- ---------------------------------------------------------------------------
local adopted = 0

local parked = redis.call('LRANGE', KEYS[1], 0, limit - 1)

for i = 1, #parked do
    -- 'NX' = only add if this member is absent; never touch an existing score.
    -- That single flag is what makes this phase safe to run concurrently with
    -- workers picking jobs up: it can create a missing stamp but can never
    -- overwrite a real one with a later time, which would make a genuinely
    -- abandoned job look permanently fresh and leave it stranded forever.
    adopted = adopted + redis.call('ZADD', KEYS[2], 'NX', ARGV[1], parked[i])
end

-- ---------------------------------------------------------------------------
-- PHASE 2 -- RECLAIM WHAT IS STALE
--
-- The cutoff is derived, not chosen: a delivery is hard-capped at 10 seconds
-- (9c), so a job picked up 60 seconds ago cannot still be in progress. That is
-- what makes this a proof rather than a guess, and it is why no worker needs to
-- announce that it is alive -- time is the liveness signal.
--
-- '-inf' as the lower bound because there is no floor: a job stamped days ago
-- is more abandoned, not less.
--
-- 🔴 THE LIMIT IS NOT OPTIONAL, for the same reason as in claim-due-retries.lua.
-- Redis runs this on its single thread, so for as long as it takes, every other
-- client is blocked -- including the producer's LPUSH inside a live HTTP
-- request. A node failure that strands thousands of jobs must become many short
-- passes rather than one long stall.
-- ---------------------------------------------------------------------------
local stale = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[2], 'LIMIT', 0, limit)

local reclaimed = {}
local pruned = 0

for i = 1, #stale do
    local job = stale[i]

    -- 🔴 THE CONDITION THAT DOES TWO JOBS AT ONCE.
    --
    -- LREM matches on exact byte equality and returns how many it removed. The
    -- job string here came out of the index, and the index stores the same bytes
    -- the processing list holds, so a match means the job is genuinely still
    -- parked and this sweep has just claimed it.
    --
    -- A zero means the job is NOT in the processing list any more: it was
    -- completed, retried or dead-lettered, and only its index entry was left
    -- behind -- by a crash between the LREM and the ZREM on the release path, or
    -- by an index write that failed while the delivery succeeded (14a). Pushing
    -- it would re-deliver an event that is already finished.
    --
    -- So the same branch both reclaims abandoned work and prunes stale
    -- bookkeeping, and the processing list is the thing that decides which is
    -- which. That is what "the list is the source of truth" means in practice.
    local removed = redis.call('LREM', KEYS[1], 1, job)

    -- Either way the index entry goes: the job is now on the queue (where a
    -- fresh pickup will stamp it again) or it was never parked to begin with.
    redis.call('ZREM', KEYS[2], job)

    if removed > 0 then
        -- LPUSH, matching the producer (8c) and the retry scheduler (12d). The
        -- worker takes from the other end, so a reclaimed job joins the BACK of
        -- the FIFO line and is indistinguishable from a new event -- the
        -- delivery path never learns that recovery exists.
        --
        -- The alternative, RPUSH, would put it at the head on the argument that
        -- it is the oldest work in the system. Rejected for consistency with the
        -- retry path, and because once 60 seconds have already been spent
        -- detecting the failure, queue position is noise.
        --
        -- ⚠️ The job goes back at the SAME attempt number. Nothing was spent:
        -- the subscriber may never have been contacted at all. The honest
        -- consequence is that a job which repeatedly kills workers would be
        -- reclaimed forever, since the attempt ceiling never advances. Bounding
        -- that needs a SECOND counter -- how many times this was handed to a
        -- worker, as distinct from how many times we called the subscriber --
        -- which is exactly what SQS's maxReceiveCount and Redis Streams'
        -- delivery_count are. Deliberately out of scope; every reclaim is logged
        -- at WARN so the loop is at least visible.
        redis.call('LPUSH', KEYS[3], job)
        reclaimed[#reclaimed + 1] = job
    else
        pruned = pruned + 1
    end
end

return { adopted, pruned, reclaimed }
