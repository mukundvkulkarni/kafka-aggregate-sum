package aggregate.sum;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import io.confluent.developer.avro.Transactions;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class TxnTotalConsumer {

	public static void main(String[] args) throws Exception {

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("group.id", "test");
		props.setProperty("enable.auto.commit", "true");
		props.setProperty("auto.commit.interval.ms", "1000");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
		props.setProperty(SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");
		//KafkaAvroDeserializer k = null;
		KafkaConsumer<String, Transactions> consumer = new KafkaConsumer<>(props);
		//consumer.
		consumer.subscribe(Arrays.asList("transactions-sum"));
		while (true) {
			ConsumerRecords<String, Transactions> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, Transactions> record : records)
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
		}
	}
	
	  private static  SpecificAvroSerde<Transactions> transactionsSerde(final Properties envProps) {
		    final SpecificAvroSerde<Transactions> serde = new SpecificAvroSerde<>();
		    Map<String, String> config = new HashMap<>();
		    config.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));
		    serde.configure(config, false);
		    return serde;
		  }
 
}
