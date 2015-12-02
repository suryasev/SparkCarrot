package com.salesforce.spark.carrot

import com.rabbitmq.client.QueueingConsumer
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.mutable.ArrayBuffer
import scala.reflect._
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Created by suryasev on 12/2/15.
 */
/**
 * Create an input stream that pulls messages from Rabbit MQ.
 * @param ssc_         StreamingContext object
 * @param rmqConfig   RMQConfig object containing RabbitMQ specific configurations
 * @param storageLevel Storage level to use for storing the received objects
 */
class RMQInputDStream[T: ClassTag, U <: Decoder[_] : ClassTag]
(
  @transient ssc_ : StreamingContext,
  rmqConfig: RMQConfig,
  storageLevel: StorageLevel)
  extends ReceiverInputDStream[T](ssc_) {

  def getReceiver(): Receiver[T] = new RMQReceiver[T, U](rmqConfig, storageLevel)

}


class RMQReceiver[T: ClassTag, U <: Decoder[_] : ClassTag]
(
  rmqConfig: RMQConfig,
  @transient storageLevel: StorageLevel
  )
  extends Receiver[T](storageLevel) with Logging with Serializable {

  def onStart {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      setDaemon(true)
      override def run {
        receive
      }
    }.start
  }

  def onStop {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  /**
   * Create a connection from an rmqConfig
   *
   * @param rmqConfig
   * @return rabbitMQ connection object
   */
  private def connectRabbitMq(rmqConfig: RMQConfig) = new ConnectionFactoryWithFailover(rmqConfig).newConnection

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive {

    //Handle a decoder that has an implicit ClassTag; failing this, try to use a decoder with an empty constructor
    val valueDecoder = Try(classTag[U].runtimeClass.getConstructor(classOf[ClassTag[T]]))
      .map(_.newInstance(classTag[T]).asInstanceOf[Decoder[T]])
      .getOrElse(classTag[U].runtimeClass.newInstance.asInstanceOf[Decoder[T]])

    val connection = connectRabbitMq(rmqConfig)
    val channel = connection.createChannel

    logInfo(s"Beginning processing queue: ${rmqConfig.queue}")

    val consumer = new QueueingConsumer(channel)
    channel.basicConsume(rmqConfig.queue, false, consumer)

    val delayer = new ExponentialDelay

    val cachedRecords = new ArrayBuffer[T]

    var cntRecords = 0
    var endTimestamp = System.currentTimeMillis + rmqConfig.storeTimeInterval

    def resetArrayBuffer = {
      cntRecords = 0
      endTimestamp = System.currentTimeMillis + rmqConfig.storeTimeInterval
      cachedRecords.clear()
    }



    do {
      Option(consumer.nextDelivery(1000)) match {
        case Some(response) => {

          //We don't want an obscure data error to kill the production streaming process
          Try(cachedRecords += valueDecoder.fromBytes(response.getBody))
            .recover {
            case i: DecoderException ⇒ logError(s"DecoderException: ${i.getMessage}")
            case e: java.net.ConnectException =>
              restart("Error connecting to RabbitMQ", e)
            case NonFatal(e) =>
              logWarning("Error receiving data", e)
              restart("Error receiving data", e)
          }

          //Reliable Receiver implementation that should in theory not lose any records
          cntRecords += 1
          if (System.currentTimeMillis > endTimestamp ||
            cntRecords > rmqConfig.maxElementsPerStore) {
            store(cachedRecords)
            resetArrayBuffer
            val latestDeliveryTag = response.getEnvelope.getDeliveryTag
            channel.basicAck(latestDeliveryTag, true) //This acknowledges receipt of all messages with deliveryTag <= latestDeliveryTag
            logInfo(s"Stored data for queue: ${rmqConfig.queue}")
          }

          delayer.reset()
        }
        case None => {
          logInfo(s"Channel Empty in queue: ${rmqConfig.queue}")
          delayer.pulse()
        }
      }
    }
    while (!isStopped)
    logInfo("Closing channel and connection")
    channel.close()
    connection.close()
  }

}
