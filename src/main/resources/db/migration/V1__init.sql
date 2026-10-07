-- Charging sessions as reported by chargers. Meter readings are cumulative watt-hours.
CREATE TABLE charging_session (
    session_id             TEXT PRIMARY KEY,
    charger_id             TEXT        NOT NULL,
    customer_id            TEXT        NOT NULL,
    started_at             TIMESTAMPTZ NOT NULL,
    meter_start_wh         BIGINT      NOT NULL,
    meter_last_wh          BIGINT      NOT NULL,
    last_energy_at         TIMESTAMPTZ NOT NULL,  -- last time the meter went up; idle time starts here
    stopped_at             TIMESTAMPTZ,
    meter_stop_wh          BIGINT,
    status                 TEXT        NOT NULL CHECK (status IN ('CHARGING', 'INVOICED'))
);

-- One row per event id, written in the same transaction as its effect, so a redelivered event is a no-op.
CREATE TABLE processed_event (
    event_id      TEXT PRIMARY KEY,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Invoice per session; the primary key makes double billing impossible even if two consumers race.
CREATE TABLE invoice (
    session_id        TEXT PRIMARY KEY REFERENCES charging_session (session_id),
    customer_id       TEXT        NOT NULL,
    energy_wh         BIGINT      NOT NULL,
    idle_minutes      INTEGER     NOT NULL,
    energy_cents      BIGINT      NOT NULL,
    idle_cents        BIGINT      NOT NULL,
    vat_cents         BIGINT      NOT NULL,
    total_cents       BIGINT      NOT NULL,
    currency          CHAR(3)     NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (total_cents = energy_cents + idle_cents + vat_cents)
);

-- Double-entry ledger: every journal entry's lines sum to zero (debits positive, credits negative).
CREATE TABLE journal_entry (
    id          BIGSERIAL PRIMARY KEY,
    reference   TEXT        NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE journal_line (
    id            BIGSERIAL PRIMARY KEY,
    entry_id      BIGINT  NOT NULL REFERENCES journal_entry (id),
    account       TEXT    NOT NULL,
    amount_cents  BIGINT  NOT NULL CHECK (amount_cents <> 0)
);
CREATE INDEX journal_line_account ON journal_line (account);

-- Enforced by the database, not only by the code: an entry that does not balance cannot commit.
CREATE FUNCTION journal_entry_balanced() RETURNS trigger AS $$
DECLARE s BIGINT;
BEGIN
    SELECT COALESCE(SUM(amount_cents), 0) INTO s FROM journal_line WHERE entry_id = NEW.entry_id;
    IF s <> 0 THEN
        RAISE EXCEPTION 'journal entry % does not balance: %', NEW.entry_id, s;
    END IF;
    RETURN NULL;
END $$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER journal_line_balanced
    AFTER INSERT OR UPDATE ON journal_line
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION journal_entry_balanced();

-- Transactional outbox: written with the invoice, relayed to RabbitMQ after commit.
CREATE TABLE outbox (
    id            BIGSERIAL PRIMARY KEY,
    routing_key   TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- Meter readings or stops that arrive before their session's start are parked here and replayed by the start.
CREATE TABLE parked_event (
    event_id    TEXT PRIMARY KEY,
    session_id  TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    parked_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX parked_event_session ON parked_event (session_id);
