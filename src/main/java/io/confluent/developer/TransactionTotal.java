package io.confluent.developer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import io.confluent.developer.avro.TicketSale;
import io.confluent.developer.avro.Transactions;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class TransactionTotal {


  public Properties buildStreamsProperties(Properties envProps) {
    Properties props = new Properties();

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    return props;
  }

  private SpecificAvroSerde<Transactions> transactionsSerde(final Properties envProps) {
    final SpecificAvroSerde<Transactions> serde = new SpecificAvroSerde<>();
    Map<String, String> config = new HashMap<>();
    config.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));
    serde.configure(config, false);
    return serde;
  }

	public Topology buildTopology(Properties envProps, final SpecificAvroSerde<Transactions> txnSerde) {
		final StreamsBuilder builder = new StreamsBuilder();

		final String inputTopic = envProps.getProperty("txn.input.topic.name");
		final String outputTopic = envProps.getProperty("txn.output.topic.name");

		builder.stream(inputTopic, Consumed.with(Serdes.String(), txnSerde))
// Set key to title and value to ticket value
				.map((k, v) -> new KeyValue<>(v.getGroup(), v))
// Group by title
				.groupByKey(Grouped.with(Serdes.String(),txnSerde))
// Apply SUM aggregation
				.reduce(    new Reducer<Transactions>() { /* adder */
							      @Override
							      public Transactions apply(Transactions aggValue, Transactions newValue) {
							    	  //if origianal amount is more than 5000 then reset to zero 
							    	  if(aggValue.getAmount() > 5000) {
							    		  aggValue.setAmount(0);  
							    		  aggValue.setGroup("");
							    	  }

							    	  aggValue.setAmount(aggValue.getAmount() + newValue.getAmount());
							    	  
							    	  //if new aggregated value is more than 5000 then set Txn id as the same 
							    	  if(aggValue.getAmount() > 5000) {
							    		  aggValue.setGroup("foundOne");
							    		  //aggValue.setOffset("FoundOne");
							    		  aggValue.setTxnId(newValue.getTxnId());
							    		  
							    	  }else {
							    	   //or else remove Txn id as blank s	  
							    		  aggValue.setTxnId("");
							    	  }
							    	  
							        return aggValue;
							      }
							    }
						)
// Write to stream specified by outputTopic
				.toStream().to(outputTopic, Produced.with(Serdes.String(), txnSerde));

		return builder.build();
	}

//  public Topology buildTopology(Properties envProps,
//                                final SpecificAvroSerde<Transactions> txnSerde) {
//    final StreamsBuilder builder = new StreamsBuilder();
//
//    final String inputTopic = envProps.getProperty("txn.input.topic.name");
//    final String outputTopic = envProps.getProperty("txn.output.topic.name");
//
//    builder.stream(inputTopic, Consumed.with(Serdes.String(), txnSerde))
//        // Set key to title and value to ticket value
//        .map((k, v) -> new KeyValue<>((String) v.getTxnId(), v.getAmount()))
//        // Group by title
//        .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
//        // Apply SUM aggregation
//        .reduce(Double::sum)
//        // Write to stream specified by outputTopic
//        .toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Double()));
//
//    return builder.build();
//  }

  
  public void createTopics(Properties envProps) {
    Map<String, Object> config = new HashMap<>();
    config.put("bootstrap.servers", envProps.getProperty("bootstrap.servers"));
    AdminClient client = AdminClient.create(config);

    List<NewTopic> topics = new ArrayList<>();
    topics.add(new NewTopic(
        envProps.getProperty("input.topic.name"),
        Integer.parseInt(envProps.getProperty("input.topic.partitions")),
        Short.parseShort(envProps.getProperty("input.topic.replication.factor"))));
    topics.add(new NewTopic(
        envProps.getProperty("output.topic.name"),
        Integer.parseInt(envProps.getProperty("output.topic.partitions")),
        Short.parseShort(envProps.getProperty("output.topic.replication.factor"))));

    client.createTopics(topics);
    client.close();
  }

  public Properties loadEnvProperties(String fileName) throws IOException {
    Properties envProps = new Properties();
    FileInputStream input = new FileInputStream(fileName);
    envProps.load(input);
    input.close();

    return envProps;
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      throw new IllegalArgumentException(
          "This program takes one argument: the path to an environment configuration file.");
    }

    new TransactionTotal().runRecipe(args[0]);
  }

  private void runRecipe(final String configPath) throws IOException {
    Properties envProps = this.loadEnvProperties(configPath);
    Properties streamProps = this.buildStreamsProperties(envProps);

    Topology topology = this.buildTopology(envProps, this.transactionsSerde(envProps));
    this.createTopics(envProps);

    final KafkaStreams streams = new KafkaStreams(topology, streamProps);
    final CountDownLatch latch = new CountDownLatch(1);

    // Attach shutdown handler to catch Control-C.
    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
      @Override
      public void run() {
        streams.close();
        latch.countDown();
      }
    });

    try {
      streams.start();
      latch.await();
    } catch (Throwable e) {
      System.exit(1);
    }
    System.exit(0);

  }
}

