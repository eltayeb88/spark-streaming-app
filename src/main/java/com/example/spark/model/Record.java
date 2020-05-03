package com.example.spark.model;

import java.io.Serializable;

public class Record implements Serializable{

	private static final long serialVersionUID = 3010444720433838508L;
	
	private final String transactionId;
	private final String customerId;
	private final String time;
	private final String productId;
	private final double cost;
	/**
	 * @param transactionId
	 * @param customerId
	 * @param time
	 * @param productId
	 * @param cost
	 */
	public Record(String transactionId, String customerId, String time, String productId, double cost) {
		super();
		this.transactionId = transactionId;
		this.customerId = customerId;
		this.time = time;
		this.productId = productId;
		this.cost = cost;
	}
	/**
	 * @return the transactionId
	 */
	public String getTransactionId() {
		return transactionId;
	}
	/**
	 * @return the customerId
	 */
	public String getCustomerId() {
		return customerId;
	}
	/**
	 * @return the time
	 */
	public String getTime() {
		return time;
	}
	/**
	 * @return the productId
	 */
	public String getProductId() {
		return productId;
	}
	/**
	 * @return the cost
	 */
	public double getCost() {
		return cost;
	}
	@Override
	public String toString() {
		return "Record [transactionId=" + transactionId + ", customerId=" + customerId + ", time=" + time
				+ ", productId=" + productId + ", cost=" + cost + "]";
	}
}
