package dev.kwanza.chargeledger.api;

import dev.kwanza.chargeledger.ledger.Ledger;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
class SessionController {

    private final JdbcClient jdbc;
    private final Ledger ledger;

    SessionController(JdbcClient jdbc, Ledger ledger) {
        this.jdbc = jdbc;
        this.ledger = ledger;
    }

    record SessionView(String sessionId, String chargerId, String customerId, String status,
                       long meterStartWh, long meterLastWh, Long energyWh, Integer idleMinutes,
                       Long totalCents, String currency) {}

    @GetMapping("/sessions/{id}")
    ResponseEntity<SessionView> session(@PathVariable String id) {
        return jdbc.sql("""
                SELECT s.session_id, s.charger_id, s.customer_id, s.status, s.meter_start_wh, s.meter_last_wh,
                       i.energy_wh, i.idle_minutes, i.total_cents, i.currency
                FROM charging_session s LEFT JOIN invoice i USING (session_id)
                WHERE s.session_id = ?""")
                .param(id).query(SessionView.class).optional()
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/customers/{id}/balance")
    Map<String, Object> balance(@PathVariable String id) {
        return Map.of("customerId", id, "receivableCents", ledger.balance(Ledger.receivable(id)));
    }
}
