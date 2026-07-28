package io.github.sidyn4444.webhooks.worker.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Reads and writes {@link DeliveryAttempt} rows.
 *
 * <p>An interface with no implementation anywhere in this project. At startup Spring Data JPA
 * finds it, generates a class that implements every method, and registers that as a bean — so
 * injecting this type gives you a working object that was never written by hand.
 *
 * <p><b>Extending {@code JpaRepository} supplies the whole standard set for free:</b>
 * {@code save}, {@code saveAll}, {@code findById}, {@code findAll}, {@code count},
 * {@code delete}, plus paging and sorting. The two type parameters say what it manages —
 * {@code <DeliveryAttempt, Long>} is "rows of this entity, whose primary key is a Long."
 *
 * <p>The alternative is writing the SQL and the JDBC plumbing by hand, which for a single
 * append-only table would be perhaps forty lines of {@code PreparedStatement} calls binding
 * parameters by position — with the object-to-column mapping restated at every call site and
 * checked by nothing. The cost of the free version is that the generated SQL is invisible, which
 * is easy to make slow without noticing. That cost is low here: one flat table, one insert per
 * attempt, no relationships to traverse, which is precisely the shape JPA handles best.
 *
 * <h2>Why the query methods below are declarations rather than code</h2>
 *
 * <p>Spring Data parses the <i>method name</i> and writes the query from it.
 * {@code findByEventIdOrderByAttemptNumberAsc} becomes
 * {@code SELECT … WHERE event_id = ? ORDER BY attempt_number ASC}. The name is the specification,
 * and a name it cannot parse fails loudly at startup rather than at the first call — which is the
 * right time to find out.
 *
 * <p>Both methods are here because they are the two questions this table exists to answer, and
 * defining them now means 10d has something real to verify against. Nothing calls them yet.
 */
@Repository
public interface DeliveryAttemptRepository extends JpaRepository<DeliveryAttempt, Long> {

    /**
     * The full history of one event, oldest attempt first.
     *
     * <p>This is the "what happened to event X?" query — the reason the delivery log exists at
     * all. Ordering by attempt number rather than by timestamp is deliberate: attempt order is the
     * thing actually being asked about, and it stays correct even if two rows share a timestamp or
     * the clocks on two workers disagree slightly ([[1c]] — there is no shared clock).
     */
    List<DeliveryAttempt> findByEventIdOrderByAttemptNumberAsc(String eventId);

    /**
     * How many attempts failed — the number behind an error-rate panel or an alert.
     *
     * <p>Deliberately counts on the {@code success} flag rather than on the status code. A query
     * written as {@code status_code NOT BETWEEN 200 AND 299} looks equivalent and is wrong: in SQL
     * a comparison against NULL is neither true nor false, so every timeout — the worst failures
     * in the system, the ones with no status code at all — is silently excluded from the count.
     *
     * <p>The failure would be invisible. The dashboard shows a plausible number, nobody has any
     * reason to doubt it, and the missing rows are exactly the ones an on-call engineer most needs
     * to see. Storing the boolean is what makes the obvious query the correct one.
     */
    long countBySuccessFalse();
}
