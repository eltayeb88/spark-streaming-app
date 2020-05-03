package com.example.spark.conf;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@ConfigurationProperties(prefix = "kafka")
public class KafkaConsumerConfig {

	private String bootstrapServers;
	private String topicName;
	private String consumerGroupName = "DefaultConsumerGroupName";
	private boolean enableAutoCommit = false;
	/**
	 * @return the bootstrapServers
	 */
	public String getBootstrapServers() {
		return bootstrapServers;
	}
	/**
	 * @param bootstrapServers the bootstrapServers to set
	 */
	public void setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}
	/**
	 * @return the topicName
	 */
	public String getTopicName() {
		return topicName;
	}
	/**
	 * @param topicName the topicName to set
	 */
	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}
	/**
	 * @return the consumerGroupName
	 */
	public String getConsumerGroupName() {
		return consumerGroupName;
	}
	/**
	 * @param consumerGroupName the consumerGroupName to set
	 */
	public void setConsumerGroupName(String consumerGroupName) {
		this.consumerGroupName = consumerGroupName;
	}
	/**
	 * @return the enableAutoCommit
	 */
	public boolean isEnableAutoCommit() {
		return enableAutoCommit;
	}
	/**
	 * @param enableAutoCommit the enableAutoCommit to set
	 */
	public void setEnableAutoCommit(boolean enableAutoCommit) {
		this.enableAutoCommit = enableAutoCommit;
	}
	
	
	public Map<String, Object> getKafkaParams(){
		Map<String, Object> kafkaParams = new HashMap<String, Object>();

		kafkaParams.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupName);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		return kafkaParams;
	}


//	@Bean
//	Consumer createConsumer() {
//		Properties props = new Properties();
//
//		props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
//		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupName);
//		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
//
//		final Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
//		return consumer;
//
//	}

}
