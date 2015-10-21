package com.salesforce.spark.carrot

/**
 * Created by suryasev on 4/28/15.
 */

/**
 * Config file for RabbitMQ receiver
 *
 * @param queue RabbitMQ queue
 * @param hostname RabbitMQ hostname
 * @param port RabbitMQ port
 * @param alternativeHost RabbitMQ failover host
 * @param username RabbitMQ username
 * @param password RabbitMQ password
 * @param baseDelay RabbitUtils uses an exponential decay for hitting the rabbitmq cluster in case of connectivity issues
 * @param storeTimeInterval When this many milliseconds have passed, store the data for use by downstream processing
 * @param maxElementsPerStore When this many elements have accrued, store the data for use by downstream processing
 */
case class RMQConfig(queue: String, hostname: String, port: Int, alternativeHost: String,
                     username: Option[String] = None, password: Option[String] = None, baseDelay: Int = 5,
                     storeTimeInterval: Long = 1800000, maxElementsPerStore: Int = 12500) extends Serializable