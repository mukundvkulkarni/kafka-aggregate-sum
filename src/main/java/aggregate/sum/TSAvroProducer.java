package aggregate.sum;
 

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Timestamp;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.spi.LocaleNameProvider;

import io.confluent.developer.avro.TicketSale;
import io.confluent.developer.avro.Transactions;

public class TSAvroProducer {


	public static void main(String[] args) {
		
//		txns("TxnId1",Double.parseDouble("101.20"));
//		txns("TxnId1",Double.parseDouble("102.20"));
//		txns("TxnId1",Double.parseDouble("103.20"));
//
//		txns("TxnId2",Double.parseDouble("20.20"));
//		txns("TxnId2",Double.parseDouble("21.20"));
		//ticketSales("Trishul",1100);
		ticketSales("Trishul",1);
		
		//ticketSales("Don",300);
		ticketSales("Don",1);
		
		//ticketSales("Sholay",1000);
		ticketSales("Sholay",1);
		
		
		
	}
	
	static String broker = "localhost:9092";
	static  String registery = "http://localhost:8081";
	 
	public static void ticketSales(String title, int volume ) {
		
	Properties props = new Properties();
	props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,broker );
	props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
			org.apache.kafka.common.serialization.StringSerializer.class);
	props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
			io.confluent.kafka.serializers.KafkaAvroSerializer.class);
	props.put("schema.registry.url",registery);
	KafkaProducer producer = new KafkaProducer(props);

	
	TicketSale ts = new TicketSale();
	String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
	ts.setSaleTs(timeStamp);
	ts.setTicketTotalValue(volume);
	ts.setTitle(title);
	
	
	ProducerRecord<String, TicketSale> record = new ProducerRecord<>("movie-ticket-sales", ts.getSaleTs(),ts);
	try {
		producer.send(record);
	} catch(Exception e) {
		// may need to do something with it
		System.out.println(" ????????????Exception "+ e.getLocalizedMessage()); 
	}
	// When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
	// then close the producer to free its resources.
	finally {
		producer.flush();
		producer.close();
	}

	}

		
	public static void old() {
		
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,broker );
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				org.apache.kafka.common.serialization.StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put("schema.registry.url",registery);
		KafkaProducer producer = new KafkaProducer(props);

		
	String key = "key1";
	String userSchema = "{\"type\":\"record\"," +
			"\"name\":\"myrecord\"," +
			"\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
	Schema.Parser parser = new Schema.Parser();
	Schema schema = parser.parse(userSchema);
	GenericRecord avroRecord = new GenericData.Record(schema);
	avroRecord.put("f1", "value2");

	
	ProducerRecord<Object, Object> record = new ProducerRecord<>("topic1", key, avroRecord);
	try {
		producer.send(record);
	} catch(Exception e) {
		// may need to do something with it
	}
	// When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
	// then close the producer to free its resources.
	finally {
		producer.flush();
		producer.close();
	}

	}

	
}
