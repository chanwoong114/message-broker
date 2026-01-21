package com.example.rabbitmqdemo;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {

    private final RabbitTemplate rabbitTemplate;
    private final Tracer tracer;
    private final TracingUtils tracingUtils;

    public ProducerController(RabbitTemplate rabbitTemplate, Tracer tracer, TracingUtils tracingUtils) {
        this.rabbitTemplate = rabbitTemplate;
        this.tracer = tracer;
        this.tracingUtils = tracingUtils;
    }

    @GetMapping("/order")
    public String createOrder() {
        Span span = tracer.spanBuilder("create-order").startSpan();
        try {
            rabbitTemplate.convertAndSend("orders.queue", "New RabbitMQ Order", m -> {
                tracingUtils.inject(Context.current().with(span), m.getMessageProperties());
                return m;
            });
            return "Order Sent to RabbitMQ with TraceID: " + span.getSpanContext().getTraceId();
        } finally {
            span.end();
        }
    }
}
