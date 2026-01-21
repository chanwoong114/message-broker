package com.example.rabbitmqdemo;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.stereotype.Component;

@Component
public class TracingUtils {

    public void inject(Context context, MessageProperties props) {
        GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .inject(context, props, setter);
    }

    public Context extract(MessageProperties props) {
        return GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), props, getter);
    }

    private static final TextMapSetter<MessageProperties> setter = (carrier, key, value) -> 
        carrier.setHeader(key, value);

    private static final TextMapGetter<MessageProperties> getter = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(MessageProperties carrier) {
            return carrier.getHeaders().keySet();
        }
        @Override
        public String get(MessageProperties carrier, String key) {
            Object value = carrier.getHeader(key);
            return value != null ? value.toString() : null;
        }
    };
}
