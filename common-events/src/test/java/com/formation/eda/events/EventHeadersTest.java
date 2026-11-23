package com.formation.eda.events;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Un nom de header est un element de contrat : producteur et consumer n'ont aucun autre moyen
 * de se mettre d'accord dessus, et une faute de frappe ne se voit pas — le header est juste
 * absent, et le consumer continue sans dedupliquer. D'ou des assertions sur les chaines
 * exactes et non sur l'existence des constantes.
 */
class EventHeadersTest {

    @Test
    @DisplayName("les noms de headers sont ceux du contrat")
    void headerNamesMatchTheContract() {
        assertThat(EventHeaders.EVENT_ID).isEqualTo("eventId");
        assertThat(EventHeaders.EVENT_TYPE).isEqualTo("eventType");
        assertThat(EventHeaders.CORRELATION_ID).isEqualTo("correlationId");
        assertThat(EventHeaders.ORIGIN_TOPIC).isEqualTo("x-origin-topic");
        assertThat(EventHeaders.ORIGIN_TIMESTAMP).isEqualTo("x-origin-timestamp");
        assertThat(EventHeaders.RETRY_STAGE).isEqualTo("x-retry-stage");
        assertThat(EventHeaders.FAILED_AT).isEqualTo("x-failed-at");
        assertThat(EventHeaders.FAIL_TIMES).isEqualTo("x-fail-times");
        assertThat(EventHeaders.FAIL_FATAL).isEqualTo("x-fail-fatal");
    }

    @Test
    @DisplayName("les headers de diagnostic sont prefixes x- et les headers de contrat ne le sont pas")
    void diagnosticHeadersArePrefixed() {
        assertThat(EventHeaders.EVENT_ID).doesNotStartWith("x-");
        assertThat(EventHeaders.EVENT_TYPE).doesNotStartWith("x-");
        assertThat(EventHeaders.CORRELATION_ID).doesNotStartWith("x-");

        assertThat(EventHeaders.ORIGIN_TOPIC).startsWith("x-");
        assertThat(EventHeaders.ORIGIN_TIMESTAMP).startsWith("x-");
        assertThat(EventHeaders.RETRY_STAGE).startsWith("x-");
        assertThat(EventHeaders.FAILED_AT).startsWith("x-");
    }
}
