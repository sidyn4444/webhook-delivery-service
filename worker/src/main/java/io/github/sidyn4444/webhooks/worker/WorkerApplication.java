package io.github.sidyn4444.webhooks.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the worker service.
 *
 * <p>The worker is a <b>daemon</b>, not a web service. It pulls delivery jobs from Redis,
 * POSTs HMAC-signed webhooks to subscriber URLs, retries failures with exponential
 * backoff, and dead-letters permanent failures. Nothing ever calls it over HTTP.
 *
 * <p>That single fact is why this module depends on plain {@code spring-boot-starter}
 * rather than {@code spring-boot-starter-web}, and why {@code application.properties}
 * sets {@code spring.main.web-application-type=none}. Shipping a web server here would
 * mean a larger image, a wider attack surface, and an open port nobody should be able
 * to reach — a door in a room nobody visits.
 *
 * <p><b>A consequence worth understanding:</b> with no web server, nothing holds the
 * JVM open. A JVM exits once its last non-daemon thread finishes, and the producer keeps
 * running only because Tomcat holds one. Through Session 1 this application started, ran
 * the {@link CommandLineRunner} below, and exited within a second — the correct behaviour
 * for a program with nothing to do.
 *
 * <p>The {@code JobPoller} added in 9b now supplies that thread: its polling loop runs on a
 * dedicated non-daemon thread and never finishes, so the JVM stays up for as long as the
 * worker is consuming the queue. {@code spring.main.keep-alive=true} remains in configuration
 * as a safety net, but it is no longer the mechanism.
 *
 * <p>This class is now nothing but an entry point, which is the intended end state. All of the
 * worker's behaviour lives in components discovered by component scanning.
 */
@SpringBootApplication
public class WorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkerApplication.class, args);
    }

}
