package com.demo.streams.examples;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import com.demo.streams.examples.avro.Customer;
import com.demo.streams.examples.avro.Order;

public class GlobalKTablesExample {
	  static final String ORDER_TOPIC = "order";
	  static final String CUSTOMER_TOPIC = "customer";
	  static final String PRODUCT_TOPIC =  "product";
	  static final String CUSTOMER_STORE = "customer-store";
	  static final String PRODUCT_STORE = "product-store";
	  static final String ENRICHED_ORDER_TOPIC = "enriched-order";
	  
	public static void main(String[] args) {
	    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
	    final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:18081";
	    final KafkaStreams
	        streams =
	        createStreams(bootstrapServers, schemaRegistryUrl, "/tmp/kafka-streams-global-tables");
	    // Always (and unconditionally) clean local state prior to starting the processing topology.
	    // We opt for this unconditional call here because this will make it easier for you to play around with the example
	    // when resetting the application for doing a re-run (via the Application Reset Tool,
	    // https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html).
	    //
	    // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
	    // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
	    // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
	    // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
	    // See `ApplicationResetExample.java` for a production-like example.
	    streams.cleanUp();
	    // start processing
	    streams.start();
	    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
	    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}
	
	
	  public static KafkaStreams createStreams(final String bootstrapServers,
              final String schemaRegistryUrl,
              final String stateDir) {
		  final Properties streamsConfiguration = new Properties();
		  
		  StreamsBuilder builder = new StreamsBuilder();
		  
		  return new KafkaStreams(builder.build(), streamsConfiguration);
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
