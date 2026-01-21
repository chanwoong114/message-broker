package com.example.redpandademo;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Tracer tracer;
    private final TracingUtils tracingUtils;

    public ProducerController(KafkaTemplate<String, String> kafkaTemplate, Tracer tracer, TracingUtils tracingUtils) {
        this.kafkaTemplate = kafkaTemplate;
        this.tracer = tracer;
        this.tracingUtils = tracingUtils;
    }

    @GetMapping("/order")
    public String createOrder() {
        return sendOneMessage("Single Order");
    }

    @GetMapping("/bomb")
    public String bomb() {
        for (int i = 0; i < 1000; i++) {
            sendOneMessage("Bomb Message " + i);
        }
        return "Bombed 1000 messages! Check Grafana.";
    }

    private String sendOneMessage(String content) {
        Span span = tracer.spanBuilder("produce-message").startSpan();
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>("orders.new", content);
            tracingUtils.inject(Context.current().with(span), record.headers());
            kafkaTemplate.send(record);
            return "Sent: " + content;
        } finally {
            span.end();
        }
    }
}