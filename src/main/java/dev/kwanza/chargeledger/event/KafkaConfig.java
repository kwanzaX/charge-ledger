package dev.kwanza.chargeledger.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
class KafkaConfig {

    // Topics are created explicitly: relying on broker auto-creation fails the first publish on a fresh cluster.
    @Bean
    NewTopic chargingEvents(@Value("${charge-ledger.topic}") String topic) {
        return TopicBuilder.name(topic).partitions(3).build();
    }

    @Bean
    NewTopic chargingEventsDlt(@Value("${charge-ledger.topic}") String topic) {
        return TopicBuilder.name(topic + ".DLT").partitions(3).build();
    }

    /**
     * Transient failures (a database or broker blip) are retried in place, which keeps per-session order
     * on the partition. Events that can never succeed skip the retries and go straight to
     * {@code <topic>.DLT} with the exception in the headers.
     */
    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template,
                                     @Value("${charge-ledger.retry.interval-ms}") long intervalMs,
                                     @Value("${charge-ledger.retry.attempts}") long attempts) {
        // Same partition on the DLT as on the source topic, so a session's dead events stay together.
        var recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(intervalMs, attempts));
        handler.addNotRetryableExceptions(InvalidEventException.class, JsonProcessingException.class);
        return handler;
    }
}
