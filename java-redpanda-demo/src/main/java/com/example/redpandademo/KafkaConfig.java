package com.example.redpandademo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka
public class KafkaConfig {

    /**
     * 에러 핸들러 설정:
     * 1. 메시지 처리 중 예외 발생 시 재시도 수행 (3회, 1초 간격)
     * 2. 모든 재시도 실패 시 Dead Letter Topic (DLT)으로 메시지 발행
     *    - 기본 DLT 이름: 원본토픽명.DLT (예: orders.new.DLT)
     */
    @Bean
    public CommonErrorHandler errorHandler(KafkaOperations<Object, Object> template) {
        // 1. DLQ(DLT)로 보내는 복구 로직 (Recoverer)
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> {
                    System.err.println(">>> [DLQ] Sending record to DLQ: " + record.topic() + ".DLT | Reason: " + ex.getMessage());
                    return new TopicPartition(record.topic() + ".DLT", record.partition());
                });

        // 2. 재시도 정책 (Backoff): 1초 간격, 최대 3회 시도
        FixedBackOff backOff = new FixedBackOff(1000L, 3);

        return new DefaultErrorHandler(recoverer, backOff);
    }
}
