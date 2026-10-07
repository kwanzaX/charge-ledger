package dev.kwanza.chargeledger.billing;

import java.math.BigDecimal;
import java.math.RoundingMode;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Price list for one charging network. Amounts are in major units (e.g. EUR) and converted to integer cents
 * once per invoice line, rounding half up, so the stored lines always add up to the stored total.
 */
@ConfigurationProperties("charge-ledger.tariff")
public record Tariff(
        String currency,
        BigDecimal pricePerKwh,
        BigDecimal idleFeePerMinute,
        int idleGraceMinutes,
        BigDecimal vatRate) {

    public Charges price(long energyWh, long idleMinutesTotal) {
        int billableIdle = (int) Math.max(0, idleMinutesTotal - idleGraceMinutes);
        long energyCents = cents(BigDecimal.valueOf(energyWh).movePointLeft(3).multiply(pricePerKwh));
        long idleCents = cents(idleFeePerMinute.multiply(BigDecimal.valueOf(billableIdle)));
        long vatCents = BigDecimal.valueOf(energyCents + idleCents).multiply(vatRate)
                .setScale(0, RoundingMode.HALF_UP).longValueExact();
        return new Charges(energyWh, billableIdle, energyCents, idleCents, vatCents, currency);
    }

    private static long cents(BigDecimal major) {
        return major.movePointRight(2).setScale(0, RoundingMode.HALF_UP).longValueExact();
    }

    public record Charges(long energyWh, int idleMinutes, long energyCents, long idleCents, long vatCents, String currency) {
        public long totalCents() {
            return energyCents + idleCents + vatCents;
        }
    }
}
