package com.demo.streams.examples;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import com.demo.streams.examples.avro.Customer;
import com.demo.streams.examples.avro.EnrichedOrder;
import com.demo.streams.examples.avro.Order;
import com.demo.streams.examples.avro.Product;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class GlobalKTablesExample {
	static final String ORDER_TOPIC = "order";
	static final String CUSTOMER_TOPIC = "customer";
	static final String PRODUCT_TOPIC = "product";
	static final String CUSTOMER_STORE = "customer-store";
	static final String PRODUCT_STORE = "product-store";
	static final String ENRICHED_ORDER_TOPIC = "enriched-order";

	public static void main(String[] args) {
		String bootstrapServer = args.length > 0 ? args[0] : "localhost:9092";
		final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:18081";

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
        
		final KafkaStreams streams = createStreams(bootstrapServer, schemaRegistryUrl,
				"/tmp/kafka-streams-global-tables");
		// Always (and unconditionally) clean local state prior to starting the
		// processing topology.
		// We opt for this unconditional call here because this will make it easier for
		// you to play around with the example
		// when resetting the application for doing a re-run (via the Application Reset
		// Tool,
		// https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html).
		//
		// The drawback of cleaning up local state prior is that your app must rebuilt
		// its local state from scratch, which
		// will take time and will require reading all the state-relevant data from the
		// Kafka cluster over the network.
		// Thus in a production scenario you typically do not want to clean up always as
		// we do here but rather only when it
		// is truly needed, i.e., only under certain conditions (e.g., the presence of a
		// command line flag for your app).
		// See `ApplicationResetExample.java` for a production-like example.
		streams.cleanUp();
		// start processing
		streams.start();
		// Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

	public static KafkaStreams createStreams(final String bootstrapServers, final String kafkaConfig, final String schemaRegistryUrl,
			final String stateDir) {
		final Properties streamsConfiguration = new Properties();

		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "global-tables-example");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "global-tables-example-client");
		// Where to find Kafka broker(s).
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
		// Set to earliest so we don't miss any data that arrived in the topics before
		// the process
		// started
		streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// create and configure the SpecificAvroSerdes required in this example
		final SpecificAvroSerde<Order> orderSerde = new SpecificAvroSerde<>();
		final Map<String, String> serdeConfig = Collections
				.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
		orderSerde.configure(serdeConfig, false);

		final SpecificAvroSerde<Customer> customerSerde = new SpecificAvroSerde<>();
		customerSerde.configure(serdeConfig, false);

		final SpecificAvroSerde<Product> productSerde = new SpecificAvroSerde<>();
		productSerde.configure(serdeConfig, false);

		final SpecificAvroSerde<EnrichedOrder> enrichedOrdersSerde = new SpecificAvroSerde<>();
		enrichedOrdersSerde.configure(serdeConfig, false);

		StreamsBuilder builder = new StreamsBuilder();
		
	    // Get the stream of orders
	    final KStream<Long, Order> ordersStream = builder.stream(ORDER_TOPIC, Consumed.with(Serdes.Long(), orderSerde));

	    // Create a global table for customers. The data from this global table
	    // will be fully replicated on each instance of this application.
	    final GlobalKTable<Long, Customer>
	        customers =
	        builder.globalTable(CUSTOMER_TOPIC, Materialized.<Long, Customer, KeyValueStore<Bytes, byte[]>>as(CUSTOMER_STORE)
	                .withKeySerde(Serdes.Long())
	                .withValueSerde(customerSerde));

	    // Create a global table for products. The data from this global table
	    // will be fully replicated on each instance of this application.
	    final GlobalKTable<Long, Product>
	        products =
	        builder.globalTable(PRODUCT_TOPIC, Materialized.<Long, Product, KeyValueStore<Bytes, byte[]>>as(PRODUCT_STORE)
	                .withKeySerde(Serdes.Long())
	                .withValueSerde(productSerde));

	    // Join the orders stream to the customer global table. As this is global table
	    // we can use a non-key based join with out needing to repartition the input stream
	    final KStream<Long, CustomerOrder> customerOrdersStream = ordersStream.join(customers,
	                                                                (orderId, order) -> order.getCustomerId(),
	                                                                (order, customer) -> new CustomerOrder(customer,
	                                                                                                       order));

	    // Join the enriched customer order stream with the product global table. As this is global table
	    // we can use a non-key based join without needing to repartition the input stream
	    final KStream<Long, EnrichedOrder> enrichedOrdersStream = customerOrdersStream.join(products,
	                                                                        (orderId, customerOrder) -> customerOrder
	                                                                            .productId(),
	                                                                        (customerOrder, product) -> new EnrichedOrder(
	                                                                            product,
	                                                                            customerOrder.customer,
	                                                                            customerOrder.order));

	    // write the enriched order to the enriched-order topic
	    enrichedOrdersStream.to(ENRICHED_ORDER_TOPIC, Produced.with(Serdes.Long(), enrichedOrdersSerde));	    

		return new KafkaStreams(builder.build(), streamsConfiguration);
		
	}
	public static KafkaStreams createStreams(final String bootstrapServers, final String schemaRegistryUrl,
			final String stateDir) {
		return createStreams(bootstrapServers, null, schemaRegistryUrl, stateDir );
	}

	private static class CustomerOrder {
		private final Customer customer;
		private final Order order;

		CustomerOrder(final Customer customer, final Order order) {
			this.customer = customer;
			this.order = order;
		}

		long productId() {
			return order.getProductId();
		}

	}

}
