package dev.kwanza.chargeledger.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Writes integration events in the caller's transaction; {@link OutboxRelay} publishes them after commit. */
@Repository
public class Outbox {

    private final JdbcClient jdbc;
    private final ObjectMapper mapper;

    public Outbox(JdbcClient jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    public void add(String routingKey, Object payload) {
        try {
            jdbc.sql("INSERT INTO outbox (routing_key, payload) VALUES (?, ?::jsonb)")
                    .params(routingKey, mapper.writeValueAsString(payload)).update();
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
