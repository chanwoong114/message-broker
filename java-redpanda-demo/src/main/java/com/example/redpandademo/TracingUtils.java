package com.example.redpandademo;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.kafka.common.header.Headers;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class TracingUtils {

    public void inject(Context context, Headers headers) {
        GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .inject(context, headers, setter);
    }

    public Context extract(Headers headers) {
        return GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), headers, getter);
    }

    private static final TextMapSetter<Headers> setter = (carrier, key, value) -> 
        carrier.add(key, value.getBytes(StandardCharsets.UTF_8));

    private static final TextMapGetter<Headers> getter = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Headers carrier) {
            return null; 
        }
        @Override
        public String get(Headers carrier, String key) {
            var header = carrier.lastHeader(key);
            if (header == null) return null;
            return new String(header.value(), StandardCharsets.UTF_8);
        }
    };
}
