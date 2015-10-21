package com.salesforce.spark.carrot

import java.net.ConnectException

import com.rabbitmq.client._
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}
import scala.util.Try

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

class ExponentialDelay(initialDelay: Long = 5, maxDelay: Long = 1000) {
  var waitTime = initialDelay

  def reset() = waitTime = initialDelay

  def pulse() = {
    Thread.sleep(waitTime)
    waitTime *= 2
    waitTime = math.min(waitTime, maxDelay)
  }
}

/**
 * Connection factory that automatically sets configs from RMQConfig and has an exponential delay and alternative host mechanic.
 *
 * See ConnectionFactory for details on RabbitMQ specific parameters.
 *
 * @param rmqConfig RMQConfig that defines RMQ behavior
 */
class ConnectionFactoryWithFailover(rmqConfig: RMQConfig) extends ConnectionFactory() {
  var ifPrimaryHost = true
  this.setHost(rmqConfig.hostname)
  this.setPort(rmqConfig.port)
  rmqConfig.username.foreach(this.setUsername _)
  rmqConfig.password.foreach(this.setPassword _)

  def toggleHost = {
    if (ifPrimaryHost) this.setHost(rmqConfig.alternativeHost)
    else if (!ifPrimaryHost) this.setHost(rmqConfig.hostname)

    ifPrimaryHost = !ifPrimaryHost
  }

  /**
   * This is an exponential delay on the calls to RabbitMQ.  If a machine is unresponsive, exponentially wait.
   * Also swap between this host and the alternative host if there is one.
   */
  override def newConnection: Connection = {
    val connectionExponentialDelay = new ExponentialDelay(1000, 600000)
    while (true) {
      try {
        val connection = super.newConnection
        connectionExponentialDelay.reset()
        return connection
      } catch {
        case ioe: ConnectException => {
          connectionExponentialDelay.pulse()
          toggleHost
        }
        case e: Exception => throw e
      }
    }
    throw new IllegalStateException("This code path should be inaccessible.")
  }
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
            case e ⇒ logError(e.getMessage)
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

