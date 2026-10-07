# charge-ledger

Bills EV charging sessions. Chargers report OCPP-style events (session started, meter values, session stopped) on Kafka; the service turns each finished session into an invoice and a balanced double-entry journal entry, then announces it on RabbitMQ. Java 21, Spring Boot 3.4, PostgreSQL 16, Apache Kafka (KRaft), RabbitMQ 3.13.

```
charger ──► Kafka charging.events (key = session id) ──► BillingService ──► PostgreSQL, one transaction:
                                   │                                         session, invoice, journal entry,
                                   └──► charging.events.DLT                  processed event id, outbox row
                                        (events that can never succeed)                │
                                                                             OutboxRelay ──► RabbitMQ billing
                                                                                              session.invoiced
```

## Pricing

A session's energy is the highest meter reading minus the start reading. Idle time runs from the moment the meter stopped rising to the moment the car was unplugged; the first 15 minutes are free. With the default tariff (0.45 EUR/kWh, 0.10 EUR per idle minute, 23% VAT), 22.5 kWh with 40 minutes plugged in after the battery was full costs:

| line | amount |
|---|---|
| energy, 22.5 kWh | 10.13 |
| idle, 25 billable minutes | 2.50 |
| VAT 23% | 2.90 |
| **total** | **15.53** |

Each line is rounded to cents once (half up), and the invoice table checks that the lines add up to the total.

## Decisions

- **Idempotent consumer.** Kafka delivers at least once. Every event id is inserted into `processed_event` in the same transaction as its effect, so a redelivered or duplicated event changes nothing. A second stop for the same session, under a new event id, is ignored because the session is already invoiced, and `invoice.session_id` is a primary key, so two consumers racing cannot bill twice.
- **Order per session.** Events are keyed by session id, so one session's events stay on one partition, in order. Readings can still arrive late (chargers buffer them offline): the meter register is cumulative, so the highest reading wins, and idle time starts at the earliest time that highest reading was seen.
- **Events before their session.** A reading or stop that arrives before the session's start is parked in `parked_event` and replayed, in charger time order, when the start arrives. Retrying in place would not work here, because the start sits behind the stop on the same partition.
- **Dead-letter topic for what can never succeed.** Malformed JSON or a stop reading below the start reading go straight to `charging.events.DLT` with the exception in the headers, without retries. Transient failures (database, broker) are retried in place with backoff.
- **The ledger balances by construction.** Debits are positive and credits negative. `Ledger.post` refuses an entry that does not sum to zero, and a deferred constraint trigger in PostgreSQL refuses it again at commit, so code that bypasses the service still cannot unbalance the books. Journal references are unique, so an invoice cannot be posted twice.
- **Transactional outbox to RabbitMQ.** The `session.invoiced` message is written with the invoice, then relayed after commit. Rows are claimed with `FOR UPDATE SKIP LOCKED` and marked published only after the broker confirms (publisher confirms), so a broker outage delays messages and never loses them. Delivery is at-least-once, and each message carries a stable `messageId` (`outbox-<id>`) for consumers to dedupe.

## API

```
GET /sessions/{id}              session state and, once stopped, its invoice
GET /customers/{id}/balance     amount receivable from a customer, from the ledger
GET /actuator/health
```

## Run

```
docker compose up --build
```

Then publish JSON events to `charging.events`, keyed by session id:

```json
{"eventId":"e-1","type":"SESSION_STARTED","sessionId":"s-1","chargerId":"PT-POR-0042","customerId":"c-9","timestamp":"2026-10-07T08:00:00Z","meterWh":1000000}
{"eventId":"e-2","type":"SESSION_STOPPED","sessionId":"s-1","timestamp":"2026-10-07T09:40:00Z","meterWh":1022500}
```

## Tests

```
mvn verify
```

Unit tests cover the tariff and rounding. The integration tests run against PostgreSQL and RabbitMQ in Testcontainers and a real Kafka broker (KRaft, `@EmbeddedKafka`) in the test JVM:

- a full session is invoiced, the ledger balances and exactly one message reaches RabbitMQ;
- duplicated starts and stops, and a second stop under a new event id, bill once;
- a stop and a reading that arrive before the start are parked and replayed;
- late, out-of-order readings keep the right energy and idle time;
- an impossible event lands on the dead-letter topic and bills nothing;
- an unbalanced journal entry written straight to the database is rejected at commit.
