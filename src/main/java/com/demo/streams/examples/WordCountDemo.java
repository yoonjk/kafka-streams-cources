package com.demo.streams.examples;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class WordCountDemo {

    public static void main(String[] args) throws IOException {

        String bootstrapServer = "localhost:9092";
        String kafkaConfig = null;

        // 1. Command-line arguments
        for (int i = 0; i < args.length; i++) {

            switch (args[i]) {

                case "--bootstrap-server":
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException(
                            "Missing value for --bootstrap-server");
                    }
                    bootstrapServer = args[++i];
                    break;

                case "--kafka-config":
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException(
                            "Missing value for --kafka-config");
                    }
                    kafkaConfig = args[++i];
                    break;

                default:
                    throw new IllegalArgumentException(
                        "Unknown option: " + args[i]);
            }
        }

        // 2. Kafka Streams configuration
        Properties props = new Properties();

        if (kafkaConfig != null) {
            try (FileInputStream fis =
                     new FileInputStream(kafkaConfig)) {
                props.load(fis);
            }
        }

        props.put(
            StreamsConfig.APPLICATION_ID_CONFIG,
            "kafka-wordcount-examples"
        );

        props.put(
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
            bootstrapServer
        );

        props.put(
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.String().getClass()
        );

        props.put(
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.String().getClass()
        );

        // 3. Build topology
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source =
            builder.stream("streams-plaintext-input");

        KTable<String, Long> counts = source
            .flatMapValues(value ->
                java.util.Arrays.asList(
                    value.toLowerCase().split("\\W+")
                )
            )
            .filter((key, word) -> !word.isEmpty())
            .groupBy((key, word) -> word)
            .count();

        counts.toStream().to(
            "streams-plaintext-output",
            Produced.with(
                Serdes.String(),
                Serdes.Long()
            )
        );

        // 4. Start Kafka Streams
        KafkaStreams streams =
            new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(
            new Thread(streams::close)
        );

        streams.start();
    }
}
