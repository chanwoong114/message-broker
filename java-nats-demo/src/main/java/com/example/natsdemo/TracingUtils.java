package com.example.natsdemo;

import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.springframework.stereotype.Component;

@Component
public class TracingUtils {

    public void inject(Context context, Headers headers) {
        GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .inject(context, headers, setter);
    }

    public Context extract(Message msg) {
        return GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), msg.getHeaders(), getter);
    }

    private static final TextMapSetter<Headers> setter = (carrier, key, value) -> 
        carrier.add(key, value);

    private static final TextMapGetter<Headers> getter = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Headers carrier) {
            return carrier.keySet();
        }
        @Override
        public String get(Headers carrier, String key) {
            if (carrier == null) return null;
            return carrier.getFirst(key);
        }
    };
}
