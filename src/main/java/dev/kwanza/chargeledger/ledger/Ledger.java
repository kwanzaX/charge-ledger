package dev.kwanza.chargeledger.ledger;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Double-entry ledger. Debits are positive, credits negative; every entry sums to zero.
 * The code checks it before writing and a deferred constraint trigger checks it again at commit.
 */
@Repository
public class Ledger {

    private final JdbcClient jdbc;

    public Ledger(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Posts one journal entry. {@code reference} is unique, so the same business event cannot be posted twice. */
    public long post(String reference, Map<String, Long> lines) {
        Map<String, Long> nonZero = new LinkedHashMap<>();
        lines.forEach((account, cents) -> { if (cents != 0) nonZero.merge(account, cents, Long::sum); });
        long sum = nonZero.values().stream().mapToLong(Long::longValue).sum();
        if (nonZero.isEmpty() || sum != 0) {
            throw new IllegalArgumentException("unbalanced entry " + reference + ": " + nonZero);
        }
        long entryId = jdbc.sql("INSERT INTO journal_entry (reference) VALUES (?) RETURNING id")
                .param(reference).query(Long.class).single();
        nonZero.forEach((account, cents) ->
                jdbc.sql("INSERT INTO journal_line (entry_id, account, amount_cents) VALUES (?, ?, ?)")
                        .params(entryId, account, cents).update());
        return entryId;
    }

    public long balance(String account) {
        return jdbc.sql("SELECT COALESCE(SUM(amount_cents), 0) FROM journal_line WHERE account = ?")
                .param(account).query(Long.class).single();
    }

    /** Sum of every line in the ledger; anything other than zero means the books are broken. */
    public long trialBalance() {
        return jdbc.sql("SELECT COALESCE(SUM(amount_cents), 0) FROM journal_line").query(Long.class).single();
    }

    public static String receivable(String customerId) {
        return "receivable:customer:" + customerId;
    }

    public static final String REVENUE_ENERGY = "revenue:energy";
    public static final String REVENUE_IDLE = "revenue:idle";
    public static final String VAT_PAYABLE = "liability:vat";
}
