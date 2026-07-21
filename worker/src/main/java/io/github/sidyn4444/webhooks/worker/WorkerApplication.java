package io.github.sidyn4444.webhooks.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

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
 * JVM open. The producer stays running because Tomcat keeps a non-daemon thread alive;
 * this application starts, runs the {@link CommandLineRunner} below, and exits cleanly.
 * That is the correct Week-1 behaviour and is exactly what proves the wiring works.
 * In Week 2 the job-polling loop becomes the thing that keeps the process alive.
 */
@SpringBootApplication
public class WorkerApplication {

    private static final Logger log = LoggerFactory.getLogger(WorkerApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(WorkerApplication.class, args);
    }

    /**
     * Runs once after the Spring context is fully built, then returns.
     *
     * <p>Week-1 scaffolding only: it proves the context started, configuration resolved,
     * and dependency injection is working, without a queue to poll yet. Week 2 replaces
     * this with the RPOPLPUSH/ack loop from the worker protocol.
     */
    @Bean
    CommandLineRunner startupProbe() {
        return args -> {
            log.info("Worker started — Spring context up, no web server (daemon mode).");
            log.info("Week 2 replaces this with the Redis job-polling loop.");
        };
    }
}
