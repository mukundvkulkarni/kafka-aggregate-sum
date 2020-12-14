package aggregate.sum;
 

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.spi.LocaleNameProvider;

import io.confluent.developer.TransactionTotal;
import io.confluent.developer.TxnProperties;
import io.confluent.developer.avro.TicketSale;
import io.confluent.developer.avro.Transactions;

public class TxnAvroProducer {

    private static final Random randomGenerator = new Random(); 
//	static String broker = "localhost:9092";
//	static  String registery = "http://localhost:8081";
     String topic = "transactions";
	static long start =  System.currentTimeMillis();  

	KafkaProducer producer; 
	String[] transactionIds = {"A","B","C","D","E"} ;  

	public static void main(String[] args)throws Exception {

		TxnAvroProducer tap = new TxnAvroProducer();
		int numberOfRecord = 10;
		if(args.length >0) {
			numberOfRecord = Integer.parseInt(args[0]) ;
		}
		tap.generateAndSend(numberOfRecord);
		tap.close();
	}

	TxnProperties pHelper  = new TxnProperties();
	public TxnAvroProducer() throws IOException {
		
		//load env properties 
		Properties props = pHelper.loadEnvProperties();
		//build stream properties 
		props.putAll(pHelper.buildStreamsProperties(props));

		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				org.apache.kafka.common.serialization.StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		topic = props.getProperty("txn.input.topic.name");
		producer = new KafkaProducer(props);
	}

    public void generateAndSend(int numberOfMessages) {
    	
    	for (int i=0 ;i< numberOfMessages ; i++) {
    		Transactions   t =getTransaction(i); 
    		ProducerRecord<String, Transactions> record = new ProducerRecord<>(topic, t.getTxnId(), t);
    		try {
    			producer.send(record);
    		} catch(Exception e) {
    			// may need to do something with it
    			System.out.println(" ????????????Exception "+ e.getLocalizedMessage()); 
    		}
    	}
    }
    
    public void close() {
    	
		try {
			producer.flush();
			producer.close();
		} catch (Exception e) {

			e.printStackTrace();
		}
    }
    
	private   Transactions getTransaction(int i ) {
		
		//BigDecimal amount = new BigDecimal(Math.random(),MathContext.DECIMAL32);
		
		boolean type =  randomGenerator.nextBoolean();    
		
		Transactions t = new Transactions();
		t.setTime(System.currentTimeMillis()- start );
		t.setType(type);
		double amount = randomGenerator.nextDouble();
		if (amount < 10 ) {
			amount = amount *10;  
		} 
		
//		if(! type) {
//			amount = - amount;  
//		}
		t.setAmount(amount);
		t.setSoruce("Avro Class");
		t.setGroup("0");
		t.setOffset("");
//		int c = i % transactionIds.length ; 
//		if(c >=0 && c < transactionIds.length) {
//			t.setTxnId(transactionIds[c]);
//		}else {
//			t.setTxnId("error");
//		}
		t.setTxnId(UUID.randomUUID().toString());
		System.out.println("sending event "+t);

		return t;
	}
	
	  
}
