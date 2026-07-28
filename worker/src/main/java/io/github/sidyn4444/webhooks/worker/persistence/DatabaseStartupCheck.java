package io.github.sidyn4444.webhooks.worker.persistence;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;

/**
 * Proves at startup that the database is genuinely reachable, and refuses to start if it is not.
 *
 * <p>The failure this prevents has appeared twice already in this project, with Redis in 8a and
 * again in 9a: <b>an application that starts perfectly is not an application that is connected.</b>
 * Configuration is only text until something uses it, so a wrong host, a wrong database name or a
 * stale password produces a process that boots clean, reports healthy, passes a readiness probe,
 * and fails on the first real write — hours later, on live traffic, with the deployment long since
 * declared successful.
 *
 * <p>Failing at startup instead turns that into the cheapest possible failure: a container that
 * never accepts traffic, a rollout Kubernetes halts on its own, and an error message that names the
 * cause. The general rule this is an instance of — <i>assert your dependencies at boot, because a
 * misconfiguration discovered at boot costs a restart and one discovered in production costs an
 * incident</i> — is the same reason {@code JobPoller} issues a real {@code PING} before starting
 * its loop.
 *
 * <h2>An honest note on what this class is and is not responsible for</h2>
 *
 * <p>With JPA on the classpath, Hibernate builds its {@code EntityManagerFactory} during startup
 * and asks the database for metadata to determine which SQL dialect to generate. That already
 * forces a real connection, so bad credentials would fail the boot even without this class. It is
 * therefore <b>not the only thing standing between a typo and a silent misconfiguration</b>, and
 * claiming otherwise would be overselling it.
 *
 * <p>It earns its place for three narrower reasons. It states the requirement <i>explicitly</i>
 * rather than leaving it as an emergent side effect of how Hibernate happens to detect a dialect.
 * It survives a common startup optimisation — setting {@code spring.jpa.database-platform}
 * explicitly skips that metadata lookup, at which point the implicit protection disappears without
 * anyone noticing. And it logs the server version, which is the fastest way to catch a worker
 * talking to the wrong database entirely, a case where connecting successfully is precisely the
 * problem.
 *
 * <p>The cost is one bean and a few milliseconds of startup. The alternative — no check, trusting
 * Hibernate's behaviour — is defensible, and would be the right call in a codebase that already
 * had a strong integration test covering startup against a real database. This project does not
 * have one until Session 3.
 */
@Component
public class DatabaseStartupCheck {

    private static final Logger log = LoggerFactory.getLogger(DatabaseStartupCheck.class);

    /**
     * The pool, not a connection.
     *
     * <p>A {@link DataSource} is a factory for connections rather than a connection itself. Spring
     * Boot builds one — a HikariCP pool — from the {@code spring.datasource.*} properties. Asking
     * it for a connection either hands back an idle pooled one or opens a new one, which is what
     * makes the check below a real network operation rather than a lookup of cached state.
     */
    private final DataSource dataSource;

    public DatabaseStartupCheck(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Runs once, after this bean is constructed, during application startup.
     *
     * <p>Throwing from a {@code @PostConstruct} method fails context startup, which terminates the
     * JVM with a non-zero exit code. That is the entire mechanism: there is no way to "fail" a
     * startup check other than to prevent the application from existing.
     */
    @PostConstruct
    void verifyConnection() {
        // try-with-resources: the connection is returned to the pool automatically at the end of
        // the block, including when an exception is thrown. Leaking pooled connections is the
        // classic way to exhaust a pool — after five leaks this worker would hang for five seconds
        // on every subsequent query and then fail, with nothing in the logs pointing here.
        try (Connection connection = dataSource.getConnection()) {

            // isValid() asks the driver to confirm the connection is actually usable, with a
            // timeout in seconds. It is stronger than merely holding a Connection object, because
            // a pooled connection can be handed out after the server has closed it — the object
            // exists and every field looks right, and the first statement fails.
            if (!connection.isValid(2)) {
                throw new IllegalStateException("Obtained a database connection, but it is not valid");
            }

            // Metadata describes the server on the other end. Logging it is what turns "connected"
            // into "connected to WHAT" — the version, and the URL with no credentials in it.
            DatabaseMetaData meta = connection.getMetaData();
            log.info("Database reachable: {} {} at {}",
                    meta.getDatabaseProductName(),
                    meta.getDatabaseProductVersion(),
                    meta.getURL());

        } catch (Exception e) {
            // Rethrown, deliberately, rather than logged and swallowed. Logging an error and
            // carrying on would produce exactly the failure this class exists to prevent: a
            // running worker with no database, delivering webhooks whose outcomes are recorded
            // nowhere. The message names the URL so the log line is self-contained; the cause is
            // attached so the underlying SQLException is not lost.
            throw new IllegalStateException(
                    "Cannot reach the delivery-log database — refusing to start. Check "
                            + "POSTGRES_URL / POSTGRES_USER / POSTGRES_PASSWORD and that Postgres is running.",
                    e);
        }
    }
}
