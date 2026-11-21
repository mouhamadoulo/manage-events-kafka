package com.formation.eda.analytics;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Verifie que le contexte Spring demarre. Aucun broker requis : aucun listener n'est
 * encore declare, et le producer Kafka se connecte paresseusement.
 */
@SpringBootTest
class AnalyticsServiceApplicationTest {

    @Test
    void contextLoads() {
    }
}
