package com.salesforce.spark.carrot

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._

import scala.reflect.ClassTag

/**
 * Created by suryasev on 4/28/15.
 *
 * Modeled upon org.apache.spark.streaming.kafka.KafkaUtils
 */
object RabbitUtils {
  /**
   * Create an input stream that pulls messages from Rabbit MQ.
   * @param ssc         StreamingContext object
   * @param rmqConfig   RMQConfig object containing RabbitMQ specific configurations
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createStream[T: ClassTag, U <: Decoder[_] : ClassTag]
  (
    ssc: StreamingContext,
    rmqConfig: RMQConfig,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[T] = new RMQInputDStream[T, U](ssc, rmqConfig, storageLevel)
}

