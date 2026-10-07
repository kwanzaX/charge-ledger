package dev.kwanza.chargeledger;

import static org.assertj.core.api.Assertions.assertThat;

import dev.kwanza.chargeledger.billing.Tariff;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class TariffTest {

    private final Tariff tariff = new Tariff("EUR", new BigDecimal("0.45"), new BigDecimal("0.10"), 15, new BigDecimal("0.23"));

    @Test
    void pricesEnergyPerKwhAndAddsVat() {
        Tariff.Charges c = tariff.price(22_500, 0);   // 22.5 kWh

        assertThat(c.energyCents()).isEqualTo(1013);   // 10.125 EUR rounds half up to 10.13
        assertThat(c.idleCents()).isZero();
        assertThat(c.vatCents()).isEqualTo(233);       // 23% of 10.13 = 2.3299
        assertThat(c.totalCents()).isEqualTo(1246);
    }

    @Test
    void idleFeeStartsAfterTheGracePeriod() {
        assertThat(tariff.price(10_000, 15).idleMinutes()).isZero();
        Tariff.Charges c = tariff.price(10_000, 40);

        assertThat(c.idleMinutes()).isEqualTo(25);
        assertThat(c.idleCents()).isEqualTo(250);
    }

    @Test
    void roundsHalfUpOnlyOncePerLine() {
        assertThat(tariff.price(11_111, 0).energyCents()).isEqualTo(500);   // 4.99995 EUR
        assertThat(tariff.price(11, 0).energyCents()).isZero();             // 0.00495 EUR
    }

    @Test
    void zeroEnergyCostsNothing() {
        assertThat(tariff.price(0, 0).totalCents()).isZero();
    }
}
