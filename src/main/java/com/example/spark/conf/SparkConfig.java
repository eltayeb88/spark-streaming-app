package com.example.spark.conf;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@ConfigurationProperties(prefix = "spark")
public class SparkConfig {

	private String appName;
	private String master;
	private Long window;

	/**
	 * @return the appName
	 */
	public String getAppName() {
		return appName;
	}

	/**
	 * @param appName the appName to set
	 */
	public void setAppName(String appName) {
		this.appName = appName;
	}

	/**
	 * @return the master
	 */
	public String getMaster() {
		return master;
	}

	/**
	 * @param master the master to set
	 */
	public void setMaster(String master) {
		this.master = master;
	}

	/**
	 * @return the window
	 */
	public Long getWindow() {
		return window;
	}

	/**
	 * @param window the window to set
	 */
	public void setWindow(Long window) {
		this.window = window;
	}

	@Bean
	JavaSparkContext sparkContext() {
		SparkConf conf = new SparkConf();
		conf.setAppName(appName);
		conf.setMaster(master);
		return new JavaSparkContext(conf);
	}

	@Bean
	JavaStreamingContext sparkStream(JavaSparkContext sc) {
		JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.milliseconds(window));
		return jssc;
	}
}
