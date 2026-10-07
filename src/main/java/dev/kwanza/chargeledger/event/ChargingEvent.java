package dev.kwanza.chargeledger.event;

import java.time.Instant;

/**
 * An event reported by a charger, modelled on OCPP StartTransaction / MeterValues / StopTransaction.
 * {@code meterWh} is the charger's cumulative energy register in watt-hours, not a delta.
 */
public record ChargingEvent(
        String eventId,
        Type type,
        String sessionId,
        String chargerId,
        String customerId,
        Instant timestamp,
        long meterWh) {

    public enum Type { SESSION_STARTED, METER_VALUES, SESSION_STOPPED }

    /** Rejects events that can never be processed, so they go to the dead-letter topic instead of being retried. */
    public void validate() {
        if (blank(eventId) || type == null || blank(sessionId) || timestamp == null) {
            throw new InvalidEventException("eventId, type, sessionId and timestamp are required");
        }
        if (type == Type.SESSION_STARTED && (blank(chargerId) || blank(customerId))) {
            throw new InvalidEventException("SESSION_STARTED needs chargerId and customerId");
        }
        if (meterWh < 0) {
            throw new InvalidEventException("meterWh must not be negative");
        }
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }
}
