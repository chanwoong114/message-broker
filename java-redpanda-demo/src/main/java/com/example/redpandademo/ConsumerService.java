package com.example.redpandademo;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Objects;

@RestController
public class ConsumerService {

    private final Tracer tracer;
    private final TracingUtils tracingUtils;
    private final KafkaListenerEndpointRegistry registry;

    public ConsumerService(Tracer tracer, TracingUtils tracingUtils, KafkaListenerEndpointRegistry registry) {
        this.tracer = tracer;
        this.tracingUtils = tracingUtils;
        this.registry = registry;
    }

    @KafkaListener(id = "myListener", topics = "orders.new", groupId = "redpanda-group")
    public void consume(ConsumerRecord<String, String> record) {
        Context parentContext = tracingUtils.extract(record.headers());
        Span span = tracer.spanBuilder("process-message")
                .setParent(parentContext)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            // System.out.println("Processed: " + record.value());
        } finally {
            span.end();
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