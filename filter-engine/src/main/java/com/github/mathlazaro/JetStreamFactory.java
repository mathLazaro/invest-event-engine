package com.github.mathlazaro;

import io.synadia.flink.message.SourceConverter;
import io.synadia.flink.source.JetStreamSource;
import io.synadia.flink.source.JetStreamSourceBuilder;
import io.synadia.flink.source.JetStreamSubjectConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

public class JetStreamFactory {

    private static final Properties connectionProperties = new Properties();

    static {
        connectionProperties.put("io.nats.client.url", "nats://localhost:4222");
    }


    public static <T> JetStreamSource<T> sourceFromJsonConfig(String jsonPath, SourceConverter<T> converter) {

        JetStreamSubjectConfiguration jsConfig;
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(jsonPath)) {
            if (is == null) throw new ExceptionInInitializerError(String.format("%s not found in resources", jsonPath));
            String configJson = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            jsConfig = JetStreamSubjectConfiguration.fromJson(configJson);
        } catch (IOException e) {
            throw new ExceptionInInitializerError(String.format("Error on reading %s", jsonPath));
        }

        return new JetStreamSourceBuilder<T>()
                .sourceConverter(converter)
                .connectionProperties(connectionProperties)
                .addSubjectConfigurations(new ArrayList<>(Collections.singletonList(jsConfig)))
                .build();
    }


}
