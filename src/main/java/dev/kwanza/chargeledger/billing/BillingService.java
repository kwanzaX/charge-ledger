package dev.kwanza.chargeledger.billing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.kwanza.chargeledger.event.ChargingEvent;
import dev.kwanza.chargeledger.event.InvalidEventException;
import dev.kwanza.chargeledger.ledger.Ledger;
import dev.kwanza.chargeledger.outbox.Outbox;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Turns charger events into sessions, and a finished session into an invoice, a journal entry and an
 * outbox message, all in one transaction. Delivery from Kafka is at-least-once, so every event id is
 * recorded in the same transaction as its effect: a redelivered event changes nothing. A reading or stop
 * that arrives before its session's start is parked and replayed when the start arrives.
 */
@Service
public class BillingService {

    private static final Logger log = LoggerFactory.getLogger(BillingService.class);

    private final JdbcClient jdbc;
    private final Ledger ledger;
    private final Outbox outbox;
    private final Tariff tariff;
    private final ObjectMapper mapper;

    public BillingService(JdbcClient jdbc, Ledger ledger, Outbox outbox, Tariff tariff, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.ledger = ledger;
        this.outbox = outbox;
        this.tariff = tariff;
        this.mapper = mapper;
    }

    @Transactional
    public void handle(ChargingEvent e) {
        int fresh = jdbc.sql("INSERT INTO processed_event (event_id) VALUES (?) ON CONFLICT DO NOTHING")
                .param(e.eventId()).update();
        if (fresh == 0) {
            log.info("event.duplicate eventId={} sessionId={}", e.eventId(), e.sessionId());
            return;
        }
        switch (e.type()) {
            case SESSION_STARTED -> start(e);
            case METER_VALUES -> meter(e);
            case SESSION_STOPPED -> stop(e);
        }
    }

    private void start(ChargingEvent e) {
        int inserted = jdbc.sql("""
                INSERT INTO charging_session (session_id, charger_id, customer_id, started_at, meter_start_wh,
                                              meter_last_wh, last_energy_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'CHARGING') ON CONFLICT (session_id) DO NOTHING""")
                .params(e.sessionId(), e.chargerId(), e.customerId(), ts(e.timestamp()), e.meterWh(), e.meterWh(), ts(e.timestamp()))
                .update();
        if (inserted == 0) {
            log.warn("session.start-repeated sessionId={} eventId={}", e.sessionId(), e.eventId());
            return;
        }
        replayParked(e.sessionId());
    }

    /**
     * Readings can arrive out of order. The register is cumulative, so the highest reading wins, and idle time
     * starts at the earliest timestamp seen for that highest reading.
     */
    private void meter(ChargingEvent e) {
        Optional<Session> found = lock(e.sessionId());
        if (found.isEmpty()) {
            park(e);
            return;
        }
        Session s = found.get();
        if (s.invoiced()) {
            log.info("meter.after-invoice sessionId={} eventId={}", e.sessionId(), e.eventId());
            return;
        }
        if (e.meterWh() < s.meterStartWh()) {
            throw new InvalidEventException("reading " + e.meterWh() + " Wh is below the session start " + s.meterStartWh() + " Wh");
        }
        applyReading(e.sessionId(), e.meterWh(), e.timestamp());
    }

    private void stop(ChargingEvent e) {
        Optional<Session> found = lock(e.sessionId());
        if (found.isEmpty()) {
            park(e);
            return;
        }
        Session s = found.get();
        if (s.invoiced()) {
            log.info("stop.already-invoiced sessionId={} eventId={}", e.sessionId(), e.eventId());
            return;
        }
        if (e.meterWh() < s.meterStartWh()) {
            throw new InvalidEventException("stop reading " + e.meterWh() + " Wh is below the session start " + s.meterStartWh() + " Wh");
        }
        if (e.timestamp().isBefore(s.startedAt())) {
            throw new InvalidEventException("session stops before it starts");
        }
        applyReading(e.sessionId(), e.meterWh(), e.timestamp());
        Session last = lock(e.sessionId()).orElseThrow();

        long energyWh = last.meterLastWh() - last.meterStartWh();
        long idleMinutes = Duration.between(last.lastEnergyAt(), e.timestamp()).toMinutes();
        Tariff.Charges c = tariff.price(energyWh, Math.max(0, idleMinutes));

        jdbc.sql("""
                UPDATE charging_session SET status = 'INVOICED', stopped_at = ?, meter_stop_wh = ?
                WHERE session_id = ?""").params(ts(e.timestamp()), e.meterWh(), e.sessionId()).update();
        jdbc.sql("""
                INSERT INTO invoice (session_id, customer_id, energy_wh, idle_minutes, energy_cents, idle_cents,
                                     vat_cents, total_cents, currency)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""")
                .params(e.sessionId(), s.customerId(), c.energyWh(), c.idleMinutes(), c.energyCents(), c.idleCents(),
                        c.vatCents(), c.totalCents(), c.currency())
                .update();

        Map<String, Long> lines = new LinkedHashMap<>();
        lines.put(Ledger.receivable(s.customerId()), c.totalCents());
        lines.put(Ledger.REVENUE_ENERGY, -c.energyCents());
        lines.put(Ledger.REVENUE_IDLE, -c.idleCents());
        lines.put(Ledger.VAT_PAYABLE, -c.vatCents());
        if (c.totalCents() > 0) {
            ledger.post("invoice:" + e.sessionId(), lines);
        }

        outbox.add("session.invoiced", Map.of(
                "sessionId", e.sessionId(),
                "customerId", s.customerId(),
                "energyWh", c.energyWh(),
                "idleMinutes", c.idleMinutes(),
                "totalCents", c.totalCents(),
                "currency", c.currency()));
        log.info("session.invoiced sessionId={} energyWh={} idleMinutes={} totalCents={}",
                e.sessionId(), c.energyWh(), c.idleMinutes(), c.totalCents());
    }

    private void park(ChargingEvent e) {
        jdbc.sql("INSERT INTO parked_event (event_id, session_id, payload) VALUES (?, ?, ?::jsonb)")
                .params(e.eventId(), e.sessionId(), write(e)).update();
        log.info("event.parked sessionId={} eventId={} type={}", e.sessionId(), e.eventId(), e.type());
    }

    /** Replays parked events in charger time order. An invalid one is dropped and logged; it must not undo the start. */
    private void replayParked(String sessionId) {
        List<ChargingEvent> parked = jdbc.sql("DELETE FROM parked_event WHERE session_id = ? RETURNING payload::text")
                .param(sessionId).query(String.class).list()
                .stream().map(this::read).sorted(Comparator.comparing(ChargingEvent::timestamp)).toList();
        for (ChargingEvent p : parked) {
            try {
                if (p.type() == ChargingEvent.Type.SESSION_STOPPED) stop(p); else meter(p);
            } catch (InvalidEventException ex) {
                log.warn("parked.rejected sessionId={} eventId={} reason={}", sessionId, p.eventId(), ex.getMessage());
            }
        }
    }

    private void applyReading(String sessionId, long meterWh, Instant at) {
        // SET expressions see the row as it was before the update, so the comparisons use the old meter_last_wh.
        jdbc.sql("""
                UPDATE charging_session SET
                    last_energy_at = CASE WHEN ? > meter_last_wh THEN ?
                                          WHEN ? = meter_last_wh AND ? < last_energy_at THEN ?
                                          ELSE last_energy_at END,
                    meter_last_wh  = GREATEST(meter_last_wh, ?)
                WHERE session_id = ?""")
                .params(meterWh, ts(at), meterWh, ts(at), ts(at), meterWh, sessionId)
                .update();
    }

    private Optional<Session> lock(String sessionId) {
        return jdbc.sql("""
                SELECT customer_id, started_at, meter_start_wh, meter_last_wh, last_energy_at, status = 'INVOICED' AS invoiced
                FROM charging_session WHERE session_id = ? FOR UPDATE""")
                .param(sessionId)
                .query((rs, n) -> new Session(rs.getString("customer_id"), rs.getTimestamp("started_at").toInstant(),
                        rs.getLong("meter_start_wh"), rs.getLong("meter_last_wh"),
                        rs.getTimestamp("last_energy_at").toInstant(), rs.getBoolean("invoiced")))
                .optional();
    }

    private String write(ChargingEvent e) {
        try {
            return mapper.writeValueAsString(e);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private ChargingEvent read(String json) {
        try {
            return mapper.readValue(json, ChargingEvent.class);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static Timestamp ts(Instant i) {
        return Timestamp.from(i);
    }

    private record Session(String customerId, Instant startedAt, long meterStartWh, long meterLastWh,
                           Instant lastEnergyAt, boolean invoiced) {}
}
