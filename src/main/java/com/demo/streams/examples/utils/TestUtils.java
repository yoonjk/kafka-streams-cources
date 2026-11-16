package com.demo.streams.examples.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.apache.kafka.streams.StreamsConfig;

public final class TestUtils {

    private TestUtils() {
        // Utility class
    }

    /**
     * Kafka Streams State Store용 임시 디렉터리 생성
     *
     * @return 임시 디렉터리의 절대 경로
     */
    public static String tempDirectory() {

        try {
            Path tempDir = Files.createTempDirectory(
                "kafka-streams-state-"
            );

            return tempDir.toAbsolutePath().toString();

        } catch (IOException e) {
            throw new RuntimeException(
                "Failed to create temporary directory", e
            );
        }
    }

    /**
     * Kafka Streams 설정에 임시 State Directory 적용
     *
     * @param streamsConfiguration Kafka Streams Properties
     */
    public static void configureStateDir(
            Properties streamsConfiguration) {

        streamsConfiguration.put(
            StreamsConfig.STATE_DIR_CONFIG,
            tempDirectory()
        );
    }
}

