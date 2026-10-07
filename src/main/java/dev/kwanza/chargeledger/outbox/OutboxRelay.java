package dev.kwanza.chargeledger.outbox;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Publishes outbox rows to RabbitMQ and marks them published only after the broker confirms.
 * Rows are claimed with {@code FOR UPDATE SKIP LOCKED}, so several instances can relay without
 * publishing the same row twice at the same time. Delivery is at-least-once: if the process dies
 * between the confirm and the commit, the row is sent again, with the same message id for consumers to dedupe.
 */
@Component
class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final JdbcClient jdbc;
    private final RabbitTemplate rabbit;
    private final String exchange;

    OutboxRelay(JdbcClient jdbc, RabbitTemplate rabbit, @Value("${charge-ledger.exchange}") String exchange) {
        this.jdbc = jdbc;
        this.rabbit = rabbit;
        this.exchange = exchange;
    }

    record Row(long id, String routingKey, String payload) {}

    @Scheduled(fixedDelayString = "${charge-ledger.outbox.poll-ms}")
    @Transactional
    public void relay() {
        List<Row> rows = jdbc.sql("""
                SELECT id, routing_key, payload::text AS payload FROM outbox
                WHERE published_at IS NULL ORDER BY id LIMIT 50 FOR UPDATE SKIP LOCKED""")
                .query(Row.class).list();
        for (Row row : rows) {
            Message message = MessageBuilder.withBody(row.payload().getBytes(StandardCharsets.UTF_8))
                    .setContentType(MessageProperties.CONTENT_TYPE_JSON)
                    .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                    .setMessageId("outbox-" + row.id())
                    .build();
            // Throws if the broker nacks or does not answer in time; the transaction rolls back and the row stays unpublished.
            rabbit.invoke(ops -> {
                ops.send(exchange, row.routingKey(), message);
                ops.waitForConfirmsOrDie(5_000);
                return null;
            });
            jdbc.sql("UPDATE outbox SET published_at = now() WHERE id = ?").param(row.id()).update();
            log.info("outbox.published id={} routingKey={}", row.id(), row.routingKey());
        }
    }
}
