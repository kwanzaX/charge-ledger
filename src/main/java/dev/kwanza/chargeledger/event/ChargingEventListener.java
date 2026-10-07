package dev.kwanza.chargeledger.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.kwanza.chargeledger.billing.BillingService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Charger events are keyed by session id, so all events of one session arrive in order on one partition. */
@Component
class ChargingEventListener {

    private final ObjectMapper mapper;
    private final BillingService billing;

    ChargingEventListener(ObjectMapper mapper, BillingService billing) {
        this.mapper = mapper;
        this.billing = billing;
    }

    @KafkaListener(topics = "${charge-ledger.topic}", groupId = "charge-ledger")
    void onEvent(String json) throws JsonProcessingException {
        ChargingEvent event = mapper.readValue(json, ChargingEvent.class);
        event.validate();
        billing.handle(event);
    }
}
