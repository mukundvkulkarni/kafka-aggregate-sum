package io.confluent.developer;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

public class TxnProperties {

	static String config  = "configuration\\dev.properties" ; 
	  public static void main(String[] args) throws IOException {
		  
		    if (args.length < 1) {
		    	System.out.println(" ??????????????????????????????????????????????????????? ");
		    	System.out.println(" This program takes one argument: the path to an environment configuration file. Using default: ");
		    	System.out.println(" ??????????????????????????????????????????????????????? ");
		    }else {
		    	config = args[0]; 
		    }

		    new TxnProperties().runRecipe(config);
		  }


	  private void runRecipe(final String configPath) throws IOException {
		    Properties envProps = this.loadEnvProperties();
		    System.out.println("envProps "+envProps.toString());
//		    String comments = "checing ";
//		    envProps.store(System.out, comments);
		    Properties streamProps = this.buildStreamsProperties(envProps);
//		    streamProps.store(System.out,comments);
		    System.out.println("streamProps "+streamProps.toString());

		  }
	  
	  public Properties loadEnvProperties() throws IOException {
		    Properties envProps = new Properties();
		    FileInputStream input = new FileInputStream(config);
		    envProps.load(input);
		    input.close();

		    return envProps;
		  }


		  
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

}
