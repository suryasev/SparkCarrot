package com.salesforce.spark.carrot

import java.io.{ByteArrayInputStream, IOException}

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.{SpecificDatumReader, SpecificRecord, SpecificRecordBase}

import scala.collection.immutable.HashMap
import scala.reflect._

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

class AvroDecoder[T <: SpecificRecordBase : ClassTag] extends Decoder[T] {

  private val schemaHolder = classTag[T].runtimeClass.newInstance.asInstanceOf[GenericRecord]
  private val reader = new SpecificDatumReader[T](schemaHolder.getSchema)

  override def fromBytes(bytes: Array[Byte]): T = {
    val binaryDecoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(bytes), null)
    val decodedAvro = classTag[T].runtimeClass.newInstance.asInstanceOf[T]
    reader.read(decodedAvro, binaryDecoder)
    decodedAvro
  }
}

//A magic byte allows for meta-schema evolution.
trait MagicByteMixin[T] extends Decoder[T] {

  val magicByte = 0.toByte

  abstract override def fromBytes(bytes: Array[Byte]): T = {
    val (firstByte, messageBytes) = bytes.splitAt(1)
    if (firstByte(0) != magicByte)
      throw new DecoderException(s"Magic byte is ${firstByte(0)} instead of $magicByte.  MagicByteDecoder does not know how to handle this message");
    super.fromBytes(messageBytes)
  }
}

//TODO: This might not work for GenericRecord; See project Jenga for workaround; Switching to SpecificRecord messes up something with writeMultiAvro
abstract class MagicByteAvroDecoder[T <: SpecificRecordBase : ClassTag] extends AvroDecoder[T] with MagicByteMixin[T]

class MagicByte2AvroDecoder[T <: SpecificRecordBase : ClassTag] extends MagicByteAvroDecoder[T] {
  override val magicByte = '2'.toByte
}

/**
 * Untested class for processing a magic byte + various incarnations of a backwards compatible schema
 * into object T.
 *
 * @tparam T
 */
abstract class MagicByteCompoundDecoder[T <: SpecificRecord : ClassTag] extends Decoder[T] {

  //TODO: Get this from VerifiableProperties
  val schemas: HashMap[Byte, Schema]

  val schemaReaders = schemas.mapValues(s => new SpecificDatumReader[T](s))

  override def fromBytes(bytes: Array[Byte]): T = {

    val (firstByte, messageBytes) = bytes.splitAt(1)
    val reader = schemaReaders.getOrElse(firstByte(0), {
      throw new DecoderException(s"Magic byte is not ${firstByte(0)}.  MagicByteCompoundDecoder does not know how to handle this message");
    })
    val binaryDecoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(bytes), null)
    val decodedAvro = classTag[T].runtimeClass.newInstance.asInstanceOf[T]
    reader.read(decodedAvro, binaryDecoder)
    decodedAvro
  }
}

class DecoderException(message: String) extends IOException(message)