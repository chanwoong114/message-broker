package com.example.rabbitmqdemo;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class ConsumerService {

    private final Tracer tracer;
    private final TracingUtils tracingUtils;

    public ConsumerService(Tracer tracer, TracingUtils tracingUtils) {
        this.tracer = tracer;
        this.tracingUtils = tracingUtils;
    }

    @RabbitListener(queues = "orders.queue")
    public void consume(Message message) {
        Context parentContext = tracingUtils.extract(message.getMessageProperties());
        Span span = tracer.spanBuilder("process-order")
                .setParent(parentContext)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            System.out.println("Received: " + new String(message.getBody()) + " | TraceID: " + span.getSpanContext().getTraceId());
        } finally {
            span.end();
        }
    }
}
