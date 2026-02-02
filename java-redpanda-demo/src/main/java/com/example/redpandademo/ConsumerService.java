package com.example.redpandademo;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.util.Base64;
import java.util.Objects;

@RestController
public class ConsumerService {

    private final Tracer tracer;
    private final TracingUtils tracingUtils;
    private final KafkaListenerEndpointRegistry registry;
    private final RestTemplate restTemplate;

    @Value("${jenkins.url}")
    private String jenkinsUrl;

    @Value("${jenkins.username}")
    private String jenkinsUser;

    @Value("${jenkins.token}")
    private String jenkinsToken;

    @Value("${jenkins.job.name}")
    private String jenkinsJobName;

    public ConsumerService(Tracer tracer, TracingUtils tracingUtils, KafkaListenerEndpointRegistry registry, RestTemplate restTemplate) {
        this.tracer = tracer;
        this.tracingUtils = tracingUtils;
        this.registry = registry;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(id = "myListener", topics = "orders.new", groupId = "redpanda-group")
    public void consume(ConsumerRecord<String, String> record) {
        Context parentContext = tracingUtils.extract(record.headers());
        Span span = tracer.spanBuilder("process-message")
                .setParent(parentContext)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            String message = record.value();
            System.out.println(">>> [Middleware] Received Message: " + message);

            // DLQ 테스트를 위한 강제 에러 로직
            if (message.contains("fail")) {
                throw new RuntimeException("Simulated Error for DLQ Test!");
            }

            triggerJenkinsJob(message);

        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());
            // Spring Kafka의 ErrorHandler가 잡을 수 있도록 예외를 다시 던짐
            throw e; 
        } finally {
            span.end();
        }
    }

    private void triggerJenkinsJob(String message) {
        try {
            String apiUrl = String.format("%s/job/%s/buildWithParameters?REQUEST_ID=%s&MESSAGE=%s",
                    jenkinsUrl, jenkinsJobName, System.currentTimeMillis(), message);

            HttpHeaders headers = new HttpHeaders();
            String auth = jenkinsUser + ":" + jenkinsToken;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
            headers.add("Authorization", "Basic " + encodedAuth);

            HttpEntity<String> entity = new HttpEntity<>(headers);

            System.out.println(">>> [Middleware] Triggering Jenkins: " + apiUrl);
            ResponseEntity<String> response = restTemplate.exchange(apiUrl, HttpMethod.POST, entity, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                System.out.println(">>> [Middleware] Jenkins Job Triggered Successfully! (Status: " + response.getStatusCode() + ")");
            } else {
                System.err.println(">>> [Middleware] Failed to trigger Jenkins. Status: " + response.getStatusCode());
            }

        } catch (Exception e) {
            System.err.println(">>> [Middleware] Exception triggering Jenkins: " + e.getMessage());
        }
    }

    @GetMapping("/consumer/stop")
    public String stop() {
        Objects.requireNonNull(registry.getListenerContainer("myListener")).stop();
        return "Consumer STOPPED. Lag will increase.";
    }

    @GetMapping("/consumer/start")
    public String start() {
        Objects.requireNonNull(registry.getListenerContainer("myListener")).start();
        return "Consumer STARTED. Processing backlog...";
    }
}