package com.bluedata.messages;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.*;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterKafkaProducer {

	private static final String topic = "twitter-topic";

	public static void run()
			 throws InterruptedException {

                // Read configuration files
		Properties in_config = new Properties();
                InputStream inputStream = null;
       		try {
			 inputStream = new FileInputStream("config.properties");
               		 in_config.load(inputStream);
       		} catch (IOException ex) {
            	ex.printStackTrace();
        	}

        	// get the property value and print it out
        	String kafkaBroker = in_config.getProperty("kafkaBroker");
		String clientId = in_config.getProperty("clientId");
                String consumerKey = in_config.getProperty("consumerKey");
		String consumerSecret = in_config.getProperty("consumerSecret");
		String accessToken = in_config.getProperty("accessToken");
		String accessSecret = in_config.getProperty("accessSecret");

                //Configure kafka properties 
		Properties properties = new Properties();
		properties.put("metadata.broker.list", kafkaBroker);
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("client.id", clientId);
		ProducerConfig producerConfig = new ProducerConfig(properties);
		kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
				producerConfig);

		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

		// add some track terms
		endpoint.trackTerms(Lists.newArrayList("#iHeartAwards","#GRAMMYs","#news","#BlueDataInc","#Kafka","#Hortonworks","#Cloudera","#Hadoop",
				"#BigData","#Election2016"));

		Authentication auth = new OAuth1(consumerKey, consumerSecret, accessToken,
				accessSecret);

		// Create a new BasicClient. By default gzip is enabled.
		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
				.endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		// Establish a connection
		client.connect();

		// Send blocks of messages
		for (int msgRead = 0; msgRead < 1000; msgRead++) {
			KeyedMessage<String, String> message = null;
			try {
				message = new KeyedMessage<String, String>(topic, queue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			producer.send(message);
		}
		producer.close();
		client.stop();

	}

	public static void main(String[] args) {
		try {
			TwitterKafkaProducer.run();
		} catch (InterruptedException e) {
			System.out.println(e);
		}
	}
}
