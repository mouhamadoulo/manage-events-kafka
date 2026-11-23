package com.formation.eda.notification.application;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Choisit l'injection de panne selon le profil.
 *
 * <p>WHY deux beans et non un {@code if} dans le code metier : la lecture des headers
 * {@code x-fail-*} est <b>absente</b> du chemin normal, pas desactivee par une condition qu'on
 * pourrait inverser par erreur de configuration. Le header vient du producteur, donc de
 * l'exterieur : une panne ne doit pas pouvoir etre commandee depuis un message.
 */
@Configuration(proxyBeanMethods = false)
public class FaultInjectionConfiguration {

    @Bean
    @Profile("lab")
    FaultInjection labFaultInjection() {
        return new LabFaultInjection();
    }

    @Bean
    @Profile("!lab")
    FaultInjection disabledFaultInjection() {
        return FaultInjection.DISABLED;
    }
}
