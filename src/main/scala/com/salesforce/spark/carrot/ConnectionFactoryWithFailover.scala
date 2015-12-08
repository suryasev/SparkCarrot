package com.salesforce.spark.carrot

import java.net.ConnectException

import com.rabbitmq.client.{Connection, ConnectionFactory}

/**
 * Created by suryasev on 12/2/15.
 */
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



class ExponentialDelay(initialDelay: Long = 5, maxDelay: Long = 1000) {
  var waitTime = initialDelay

  def reset() = waitTime = initialDelay

  def pulse() = {
    Thread.sleep(waitTime)
    waitTime *= 2
    waitTime = math.min(waitTime, maxDelay)
  }
}
