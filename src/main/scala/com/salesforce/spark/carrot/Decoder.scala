package com.salesforce.spark.carrot

/**
 * Created by suryasev on 4/28/15.
 *
 * Modeled on kafka.serializer.Decoder
 *
 * Not using Kafka decoder so as not to require a dependency on Kafka.  These decoders are, however, more advanced
 * as they allow reflections, removing the need for hacky avro schema tricks.
 *
 * TODO: Implement VerifiableProperties, which will also allow us to handle schema evolution to some extent
 */

/**
 * A decoder is a method of turning byte arrays into objects.
 */
trait Decoder[T] {
  def fromBytes(bytes: Array[Byte]): T
}

/**
 * The default implementation does nothing, just returns the same byte array it takes in.
 */
class DefaultDecoder extends Decoder[Array[Byte]] {
  def fromBytes(bytes: Array[Byte]): Array[Byte] = bytes
}