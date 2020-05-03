package com.example.spark;

import static com.example.spark.util.Commons.parseLine;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.example.spark.conf.KafkaConsumerConfig;
import com.example.spark.conf.SparkConfig;
import com.example.spark.model.Record;

import scala.Tuple2;

@Component
public class SparkConsumer {

	@Autowired
	private KafkaConsumerConfig kafkaConsumerConfig;

	@Autowired
	private JavaStreamingContext jsc;

	@Autowired
	private SparkConfig sparkConfig;

	@Value("${aggregate-per-customer.output:output1}")
	private String aggregatePerCustomerOutput;

	@Value("${aggregate-per-product.output:output2}")
	private String aggregatePerProductOutput;

	public void startSparkStreaming() throws InterruptedException {

		Map<String, Object> kafkaParams = kafkaConsumerConfig.getKafkaParams();
		Collection<String> topics = Arrays.asList(kafkaConsumerConfig.getTopicName());

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jsc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		processInputStream(stream);

		stream.foreachRDD(rdd -> {
			OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
			((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
		});

		jsc.start();
		jsc.awaitTermination();
	}

	private void processInputStream(JavaInputDStream<ConsumerRecord<String, String>> stream) {

		JavaDStream<Record> cachedStream = stream.map(line -> parseLine(line.value()))
				.persist(StorageLevel.MEMORY_ONLY());

		aggregateCostPerCustomer(cachedStream);

		aggregateCostPerProduct(cachedStream);
	}

	private void aggregateCostPerCustomer(JavaDStream<Record> stream) {
		JavaPairDStream<String, Double> costPerCustomer = stream
				.mapToPair(record -> new Tuple2(record.getCustomerId(), record.getCost()));

		JavaPairDStream<String, Double> aggregatedCostPerCustomer = costPerCustomer
				.reduceByKeyAndWindow(((r1, r2) -> r1 + r2), Durations.milliseconds(sparkConfig.getWindow()));

		// TODO later on collect() and wirte to file instead of generating a directory
		aggregatedCostPerCustomer.foreachRDD(r -> r.repartition(1)
				.saveAsTextFile(String.format("%s_%s", aggregatePerCustomerOutput, Instant.now().toEpochMilli())));

	}

	private void aggregateCostPerProduct(JavaDStream<Record> stream) {
		JavaPairDStream<String, Double> costPerProduct = stream
				.mapToPair(record -> new Tuple2(record.getProductId(), record.getCost()));

		JavaPairDStream<String, Double> aggregatedCostPerProduct = costPerProduct.reduceByKey((t1, t2) -> t1 + t2);

		// TODO later on collect() and wirte to file instead of generating a directory
		aggregatedCostPerProduct.foreachRDD(r -> r.repartition(1)
				.saveAsTextFile(String.format("%s_%s", aggregatePerProductOutput, Instant.now().toEpochMilli())));

	}

}
