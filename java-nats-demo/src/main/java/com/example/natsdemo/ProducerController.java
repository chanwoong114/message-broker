package com.example.natsdemo;

import io.nats.client.Connection;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {

    private final Connection natsConnection;
    private final Tracer tracer;
    private final TracingUtils tracingUtils;

    public ProducerController(Connection natsConnection, Tracer tracer, TracingUtils tracingUtils) {
        this.natsConnection = natsConnection;
        this.tracer = tracer;
        this.tracingUtils = tracingUtils;
    }

    @GetMapping("/order")
    public String createOrder() {
        Span span = tracer.spanBuilder("create-order").startSpan();
        try {
            Headers headers = new Headers();
            tracingUtils.inject(Context.current().with(span), headers);

            NatsMessage msg = NatsMessage.builder()
                    .subject("orders.new")
                    .data("New Order Created".getBytes())
                    .headers(headers)
                    .build();

            natsConnection.publish(msg);
            
            return "Order Sent with TraceID: " + span.getSpanContext().getTraceId();
        } finally {
            span.end();
        }
    }
}
