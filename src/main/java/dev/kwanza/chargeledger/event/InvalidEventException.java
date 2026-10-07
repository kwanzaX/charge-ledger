package dev.kwanza.chargeledger.event;

/** The event is malformed or contradicts what was already recorded; retrying cannot fix it. */
public class InvalidEventException extends RuntimeException {
    public InvalidEventException(String message) {
        super(message);
    }
}
