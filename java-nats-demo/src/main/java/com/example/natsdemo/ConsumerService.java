package com.example.natsdemo;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

@Service
public class ConsumerService {

    private final Connection natsConnection;
    private final Tracer tracer;
    private final TracingUtils tracingUtils;

    public ConsumerService(Connection natsConnection, Tracer tracer, TracingUtils tracingUtils) {
        this.natsConnection = natsConnection;
        this.tracer = tracer;
        this.tracingUtils = tracingUtils;
    }

    @PostConstruct
    public void subscribe() {
        Dispatcher dispatcher = natsConnection.createDispatcher((msg) -> {
            Context parentContext = tracingUtils.extract(msg);

            Span span = tracer.spanBuilder("process-order")
                    .setParent(parentContext)
                    .startSpan();

            try (Scope scope = span.makeCurrent()) {
                String data = new String(msg.getData());
                System.out.println("Received: " + data + " | TraceID: " + span.getSpanContext().getTraceId());
                Thread.sleep(100); 
            } catch (InterruptedException e) {
                // ignore
            } finally {
                span.end();
            }
        });

        dispatcher.subscribe("orders.new");
    }
}
