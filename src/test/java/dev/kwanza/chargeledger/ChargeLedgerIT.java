package dev.kwanza.chargeledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.kwanza.chargeledger.event.ChargingEvent;
import dev.kwanza.chargeledger.event.ChargingEvent.Type;
import dev.kwanza.chargeledger.ledger.Ledger;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Runs against PostgreSQL 16 and RabbitMQ 3.13 in containers and a real Kafka broker (KRaft) inside the test JVM. */
@SpringBootTest
@Testcontainers
@EmbeddedKafka(kraft = true, partitions = 3, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class ChargeLedgerIT {

    @Container @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container @ServiceConnection
    static RabbitMQContainer rabbit = new RabbitMQContainer("rabbitmq:3.13-alpine");

    @Autowired EmbeddedKafkaBroker broker;
    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired RabbitTemplate rabbitTemplate;
    @Autowired JdbcClient jdbc;
    @Autowired Ledger ledger;
    @Autowired ObjectMapper mapper;
    @Autowired TransactionTemplate tx;
    @Value("${charge-ledger.topic}") String topic;

    private static final Instant T0 = Instant.parse("2026-10-07T08:00:00Z");

    @Test
    void billsASessionPostsABalancedEntryAndPublishesOnce() throws Exception {
        String session = newId(), customer = "cust-" + newId();
        send(event(Type.SESSION_STARTED, session, customer, T0, 1_000_000));
        send(event(Type.METER_VALUES, session, customer, T0.plusSeconds(1800), 1_011_000));
        send(event(Type.METER_VALUES, session, customer, T0.plusSeconds(3600), 1_022_500));  // full at 09:00
        send(event(Type.SESSION_STOPPED, session, customer, T0.plusSeconds(3600 + 40 * 60), 1_022_500));  // unplugged 09:40

        Map<String, Object> invoice = awaitInvoice(session);
        assertThat(invoice).containsEntry("energy_wh", 22_500L).containsEntry("idle_minutes", 25);
        // 22.5 kWh x 0.45 = 10.13, idle 25 min x 0.10 = 2.50, VAT 23% of 12.63 = 2.90
        assertThat(invoice).containsEntry("energy_cents", 1013L).containsEntry("idle_cents", 250L)
                .containsEntry("vat_cents", 290L).containsEntry("total_cents", 1553L);

        assertThat(ledger.balance(Ledger.receivable(customer))).isEqualTo(1553);
        assertThat(ledger.trialBalance()).isZero();

        JsonNode published = awaitPublished(session);
        assertThat(published.get("totalCents").asLong()).isEqualTo(1553);
    }

    @Test
    void redeliveredEventsDoNotBillTwice() throws Exception {
        String session = newId(), customer = "cust-" + newId();
        ChargingEvent start = event(Type.SESSION_STARTED, session, customer, T0, 0);
        ChargingEvent stop = event(Type.SESSION_STOPPED, session, customer, T0.plusSeconds(3600), 10_000);
        send(start);
        send(start);
        send(stop);
        send(stop);
        send(event(Type.SESSION_STOPPED, session, customer, T0.plusSeconds(3700), 10_000));  // second stop, new event id

        awaitInvoice(session);
        Thread.sleep(1_500);  // give the duplicates time to be consumed
        assertThat(count("SELECT count(*) FROM invoice WHERE session_id = ?", session)).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM journal_entry WHERE reference = ?", "invoice:" + session)).isEqualTo(1);
        assertThat(ledger.balance(Ledger.receivable(customer))).isEqualTo(554);  // 10 kWh = 4.50 + 23% VAT
        assertThat(count("SELECT count(*) FROM outbox WHERE payload->>'sessionId' = ?", session)).isEqualTo(1);
    }

    @Test
    void readingsAndStopBeforeTheStartAreParkedAndReplayed() throws Exception {
        String session = newId(), customer = "cust-" + newId();
        send(event(Type.SESSION_STOPPED, session, customer, T0.plusSeconds(3600), 5_000));
        send(event(Type.METER_VALUES, session, customer, T0.plusSeconds(1200), 5_000));   // full at 08:20
        await().atMost(Duration.ofSeconds(20)).until(() -> count("SELECT count(*) FROM parked_event WHERE session_id = ?", session) == 2);
        assertThat(count("SELECT count(*) FROM invoice WHERE session_id = ?", session)).isZero();
        send(event(Type.SESSION_STARTED, session, customer, T0, 0));

        assertThat(awaitInvoice(session)).containsEntry("energy_wh", 5_000L).containsEntry("idle_minutes", 25);  // 40 min - 15
        assertThat(count("SELECT count(*) FROM parked_event WHERE session_id = ?", session)).isZero();
    }

    @Test
    void outOfOrderReadingsKeepTheHighestAndTheEarliestTimeItWasSeen() throws Exception {
        String session = newId(), customer = "cust-" + newId();
        send(event(Type.SESSION_STARTED, session, customer, T0, 0));
        send(event(Type.METER_VALUES, session, customer, T0.plusSeconds(3600), 30_000));   // full at 09:00
        send(event(Type.METER_VALUES, session, customer, T0.plusSeconds(1800), 15_000));   // late, older reading
        send(event(Type.METER_VALUES, session, customer, T0.plusSeconds(4500), 30_000));   // still full at 09:15
        send(event(Type.SESSION_STOPPED, session, customer, T0.plusSeconds(3600 + 20 * 60), 30_000));

        Map<String, Object> invoice = awaitInvoice(session);
        assertThat(invoice).containsEntry("energy_wh", 30_000L).containsEntry("idle_minutes", 5);  // 20 min idle - 15 grace
    }

    @Test
    void impossibleEventGoesToTheDeadLetterTopicWithoutRetries() throws Exception {
        String session = newId(), customer = "cust-" + newId();
        send(event(Type.SESSION_STARTED, session, customer, T0, 50_000));
        ChargingEvent bad = event(Type.SESSION_STOPPED, session, customer, T0.plusSeconds(600), 10_000);  // below start
        send(bad);

        var consumer = new DefaultKafkaConsumerFactory<>(
                KafkaTestUtils.consumerProps(broker.getBrokersAsString(), "dlt-" + newId(), "true"),
                new StringDeserializer(), new StringDeserializer()).createConsumer();
        consumer.subscribe(List.of(topic + ".DLT"));
        await().atMost(Duration.ofSeconds(20)).until(() -> {
            for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                if (r.value().contains(bad.eventId())) return true;
            }
            return false;
        });
        consumer.close();
        assertThat(count("SELECT count(*) FROM invoice WHERE session_id = ?", session)).isZero();
    }

    @Test
    void databaseRejectsAnUnbalancedEntryEvenIfTheCodeIsBypassed() {
        assertThatThrownBy(() -> tx.executeWithoutResult(s -> {
            long id = jdbc.sql("INSERT INTO journal_entry (reference) VALUES ('manual') RETURNING id").query(Long.class).single();
            jdbc.sql("INSERT INTO journal_line (entry_id, account, amount_cents) VALUES (?, 'revenue:energy', -100)").param(id).update();
        })).rootCause().hasMessageContaining("does not balance");
    }

    // --- helpers

    private ChargingEvent event(Type type, String session, String customer, Instant at, long meterWh) {
        return new ChargingEvent(newId(), type, session, "charger-7", customer, at, meterWh);
    }

    private void send(ChargingEvent e) throws Exception {
        kafkaTemplate.send(topic, e.sessionId(), mapper.writeValueAsString(e)).get();
    }

    private Map<String, Object> awaitInvoice(String session) {
        return await().atMost(Duration.ofSeconds(20)).until(
                () -> jdbc.sql("SELECT * FROM invoice WHERE session_id = ?").param(session).query().listOfRows(),
                rows -> !rows.isEmpty()).get(0);
    }

    private JsonNode awaitPublished(String session) {
        return await().atMost(Duration.ofSeconds(20)).until(() -> {
            Message m = rabbitTemplate.receive("billing.session-invoiced.notifications", 500);
            if (m == null) return null;
            JsonNode n = mapper.readTree(m.getBody());
            return session.equals(n.get("sessionId").asText()) ? n : null;
        }, n -> n != null);
    }

    private long count(String sql, Object arg) {
        return jdbc.sql(sql).param(arg).query(Long.class).single();
    }

    private static String newId() {
        return UUID.randomUUID().toString();
    }
}
