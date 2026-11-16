package com.demo.streams.examples;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import com.demo.streams.examples.utils.TestUtils;

public class WordCountLambdaExample {
	static final String inputTopic = "streams-plaintext-input";
	static final String outputTopic = "streams-wordcount-output";

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String bootstrapServer = args.length > 0 ? args[0] : "localhost:9092";
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
		// Configure the Streams application.
		final Properties streamsConfiguration = getStreamsConfiguration(bootstrapServer, kafkaConfig);
		
		// Define the processing topology of the Streams application.
		
		final StreamsBuilder builder = new StreamsBuilder();
		createWordCountStream(builder);
		final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
		
		streams.cleanUp();
		streams.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

	static Properties getStreamsConfiguration(final String bootstrapServer, final String kafkaConfig) {
		Properties streamsConfiguration = new Properties();

		try {
	        if (kafkaConfig != null) {
	            try (FileInputStream fis =
	                     new FileInputStream(kafkaConfig)) {
	            	streamsConfiguration.load(fis);
	            }
	        }
		} catch(Exception ex) {ex.printStackTrace();}

		
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "WordCountLambdaExample");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcount-lambda-example-client");
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,  Serdes.StringSerde.class);
		
		streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
		streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory());
		
		return streamsConfiguration;
	}
	
	
	static Properties getStreamsConfiguration(final String bootstrapServer) {
		
		return getStreamsConfiguration(bootstrapServer, null);
	}
	
	static void createWordCountStream(final StreamsBuilder builder) {
		final KStream<String, String> textLines = builder.stream(inputTopic);
		
		final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
		final KTable<String, Long> wordCounts = textLines
				.flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
				.groupBy((keyIgnored, word)-> word)
				.count();
		
		wordCounts.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
		
	}
}
