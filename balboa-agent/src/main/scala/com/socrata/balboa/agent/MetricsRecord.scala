package com.socrata.balboa.agent

import java.lang.{Double => JavaDouble, Long => JavaLong}
import java.util.regex.Pattern

import com.socrata.balboa.metrics.Metric
import com.socrata.balboa.metrics.Metric.RecordType
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs._
import scodec.{Codec, _}
/**
  * A Metrics Record is an immutable class that represents a metric
  * that occurred with a specific entity-id, name, value, type, and time.
  *
  * Created by michaelhotan on 2/2/16.
  * Converted from Java to Scala using scodec for decoding on 1/9/2017
  */
case class MetricsRecord(timestamp: Long, entityId: String, name: String, value: Number, metricType: RecordType)

/**
  * MetricsRecord Codec
  * <ul>
  * <li>0xff - single byte - Beginning mark of a single metrics entry</li>
  * <li>Sequence of bytes of undetermined length - A utf-8 byte String encoded to bytes of timestamp of type long.</li>
  * <li>0xfe - single byte - end of timestamp sequence</li>
  * <li>Sequence of bytes of undetermined length - A utf-8 byte String encoded to bytes of the entity id</li>
  * <li>0xfe - single byte - end of entity id byte sequence</li>
  * <li>Sequence of bytes of undetermined length - A utf-8 byte String encoded to bytes of the metric name</li>
  * <li>0xfe - single byte - end of metric name byte sequence</li>
  * <li>Sequence of bytes of undetermined length - A utf-8 byte String encoded to bytes of the metric value.
  * The metric value is of type Number</li>
  * <li>0xfe - single byte - end of metric value byte sequence</li>
  * <li>Sequence of bytes of undetermined length - A utf-8 byte String encoded to bytes of the metric type.
  * See [[com.socrata.balboa.metrics.Metric.RecordType]]</li>
  * <li>0xfe - single byte - end of metric type byte sequence</li>
  * </ul>
  * </b>
  */

object MetricsRecord {
  private val integerPattern: Pattern = "-?[0-9]+".r.pattern

  private val startByte = 0xff.toByte
  private val separatorByte = 0xfe.toByte
  private def byteTerminatedCodec[A](separatorByte: Byte, codec: Codec[A]) =
    filtered(codec, new Codec[BitVector] {
      private val separator = BitVector(separatorByte)

      override def sizeBound: SizeBound = SizeBound.unknown

      override def encode(bits: BitVector): Attempt[BitVector] = Attempt.successful(bits ++ separator)

      override def decode(bits: BitVector): Attempt[DecodeResult[BitVector]] = {
        bits.bytes.indexOfSlice(separator.bytes) match {
          case -1 => Attempt.failure(Err("Does not contain a '0x%02x' separator byte.".format(separatorByte)))
          case idx: Long => Attempt.successful(DecodeResult(bits.take(idx * 8L), bits.drop(idx * 8L + 8L)))
        }
      }
    })

  private val metricFieldStringCodec = byteTerminatedCodec(separatorByte, utf8)

  private val valueCodec: Codec[Number] = metricFieldStringCodec.exmap[Number]({ rawValue: String =>
    if (rawValue.equalsIgnoreCase("null")) {
      Attempt.successful(null) // scalastyle:ignore
    } else {
      try {
        if (integerPattern.matcher(rawValue).matches) {
          Attempt.successful(JavaLong.valueOf(rawValue))
        } else {
          Attempt.successful(JavaDouble.valueOf(rawValue))
        }
      }
      catch {
        case _: NumberFormatException =>
          Attempt.failure(Err(s"Unable to decode raw value $rawValue into Number format"))
      }
    }
  }, n => Attempt.successful(n.toString))

  private val timestampCodec: Codec[Long] = metricFieldStringCodec.xmap[Long](_.toLong, _.toString)

  private val metricTypeCodec = metricFieldStringCodec
    .xmap[RecordType](s => Metric.RecordType.valueOf(s.toUpperCase), _.toString)

  implicit val codec: Codec[MetricsRecord] = {
    ("metric start indicator" | byteTerminatedCodec(startByte, bytes).unit(ByteVector.empty)) :~>:
      ("timestamp" | timestampCodec) ::
      ("entityId" | metricFieldStringCodec) ::
      ("name" | metricFieldStringCodec) ::
      ("value" | valueCodec) ::
      ("metricType" | metricTypeCodec)
  }.as[MetricsRecord]
}
