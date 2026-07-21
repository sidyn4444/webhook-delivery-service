package io.github.sidyn4444.webhooks.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the producer service.
 *
 * <p>The producer receives events over HTTP ({@code POST /events}), validates them,
 * and enqueues delivery jobs to Redis. Week 1 scaffolding: it boots, serves HTTP,
 * and exposes a health endpoint. The controller and Redis wiring arrive in Week 2.
 *
 * <p>{@code @SpringBootApplication} is three annotations in one:
 * <ul>
 *   <li>{@code @SpringBootConfiguration} — marks this class as the source of bean definitions.
 *   <li>{@code @EnableAutoConfiguration} — the auto-configuration engine. At startup Spring Boot
 *       inspects the classpath and builds working objects from what it finds. Because
 *       spring-boot-starter-web is a dependency, it starts an embedded Tomcat on
 *       {@code server.port} without a line of setup code here.
 *   <li>{@code @ComponentScan} — scans THIS package and everything beneath it for
 *       {@code @Component} / {@code @Service} / {@code @RestController} classes to manage.
 * </ul>
 *
 * <p>That last one is why this class sits at the ROOT of the package tree
 * ({@code ...webhooks.producer}) rather than in a sub-package: component scanning starts
 * here and only looks downward. A class placed above this package would be invisible to
 * Spring and would silently never be created — a failure with no error message.
 */
@SpringBootApplication
public class ProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }
}
