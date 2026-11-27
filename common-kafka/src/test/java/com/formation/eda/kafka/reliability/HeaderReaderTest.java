package com.formation.eda.kafka.reliability;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Un header Kafka est un tableau d'octets sans type. Toute la mecanique de fiabilite de la
 * phase 4 route sur des headers : une conversion qui leve au lieu de rendre une valeur par
 * defaut ferait echouer l'intercepteur AVANT le classement d'erreur, donc sans retry ni DLT.
 */
class HeaderReaderTest {

    @Test
    @DisplayName("un header absent rend null plutot que de lever")
    void missingHeaderReadsAsNull() {
        assertThat(HeaderReader.string(new RecordHeaders(), "absent")).isNull();
    }

    @Test
    @DisplayName("un header texte est decode en UTF-8")
    void textHeaderIsDecodedAsUtf8() {
        Headers headers = headers("cle", "valeur-été");

        assertThat(HeaderReader.string(headers, "cle")).isEqualTo("valeur-été");
    }

    @Test
    @DisplayName("le dernier header d'un meme nom gagne")
    void lastValueWinsForRepeatedHeader() {
        Headers headers = new RecordHeaders();
        headers.add("cle", "premier".getBytes(StandardCharsets.UTF_8));
        headers.add("cle", "second".getBytes(StandardCharsets.UTF_8));

        // WHY: Kafka autorise plusieurs valeurs sous le meme nom, et le recoverer du framework
        // AJOUTE ses headers d'origine a chaque etage de retry au lieu de les remplacer.
        // Pour un header que nous posons nous-memes une seule fois, la derniere valeur est la
        // plus recente, donc la bonne.
        assertThat(HeaderReader.string(headers, "cle")).isEqualTo("second");
    }

    @Test
    @DisplayName("un long est lu depuis sa representation texte")
    void longIsReadFromItsTextForm() {
        assertThat(HeaderReader.longValue(headers("ts", "1795344536104"), "ts")).hasValue(1795344536104L);
    }

    @Test
    @DisplayName("un long illisible rend une valeur vide plutot que de lever")
    void unparsableLongReadsAsEmpty() {
        assertThat(HeaderReader.longValue(headers("ts", "hier"), "ts")).isEmpty();
        assertThat(HeaderReader.longValue(new RecordHeaders(), "ts")).isEmpty();
    }

    @Test
    @DisplayName("un int illisible ou absent rend le defaut")
    void unparsableIntFallsBackToDefault() {
        assertThat(HeaderReader.intValue(headers("stage", "2"), "stage", -1)).isEqualTo(2);
        assertThat(HeaderReader.intValue(headers("stage", "deux"), "stage", -1)).isEqualTo(-1);
        assertThat(HeaderReader.intValue(new RecordHeaders(), "stage", -1)).isEqualTo(-1);
    }

    private static Headers headers(String name, String value) {
        Headers headers = new RecordHeaders();
        headers.add(name, value.getBytes(StandardCharsets.UTF_8));
        return headers;
    }
}
