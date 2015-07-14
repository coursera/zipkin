package org.coursera.zipkin.courrelate

import com.google.common.net.InetAddresses
import com.twitter.logging.Logger
import com.twitter.scrooge.BinaryThriftStructSerializer
import com.twitter.util.Base64StringEncoder
import com.twitter.server.{Closer, TwitterServer}
import com.twitter.util.{Await, Closable, Future}
import com.twitter.zipkin.receiver.kafka.KafkaProcessor
import com.twitter.zipkin.receiver.kafka.KafkaSpanReceiverFactory
import com.twitter.zipkin.thriftscala.Constants
import com.twitter.zipkin.{thriftscala => thrift}
import org.apache.log4j.ConsoleAppender
import org.apache.log4j.Level
import org.apache.log4j.PatternLayout
import scala.util.Try

object StringDecoder extends KafkaProcessor.KafkaDecoder {
  private[this] val deserializer = new BinaryThriftStructSerializer[thrift.Span] {
    override val encoder = Base64StringEncoder
    val codec = thrift.Span
  }

  override def fromBytes(
    bytes: Array[Byte]): Option[List[thrift.Span]] = {

    val s = new String(bytes)
    Some(s.split(",").flatMap(d => Try(deserializer.fromString(d)).toOption).toList)
  }
}

object Main extends TwitterServer with KafkaSpanReceiverFactory with Closer {
  // initialize logging (Kafka uses log4j)
  val logger = Logger.get("Main")
  val root = org.apache.log4j.Logger.getRootLogger()
  root.addAppender(new ConsoleAppender(new PatternLayout("%r [%t] %p %c %x - %m%n")))
  root.setLevel(Level.INFO)
  

  import SpanUtils._

  val producer = new CourrelateProducer()


  def main(): Unit = {
    var (totalReceived, totalServerReceived, totalProcessed) = (0, 0, 0)

    def process(spans: Seq[thrift.Span]): Future[Unit] = {

      // we ignore client spans, as a) the timings are off and b) they're only useful in debugging
      // a handful of issues
      val serverSpans = spans.filter { span =>
        totalReceived += 1

        logger.ifDebug(s"Processing ${spans.size} spans")
        logger.ifDebug(printSpan(span))

        // we're looking for server spans that have the new set of denormalized to/From annotations
        val isServerSpan = span.annotations.exists(
          ann => (ann.value == Constants.SERVER_RECV || ann.value == Constants.SERVER_SEND))
        val isV2 = getBinaryAnnotationValue(span, "fromServiceName").isDefined

        isServerSpan && isV2
      }.map { span =>
        totalServerReceived += 1
        val spanId = new java.lang.Long(span.id)
        val parentSpanId = span.parentId

        // note: timestamp and duration are in millis!
        val recvTimestamp = span.annotations.find(_.value == Constants.SERVER_RECV).map(_.timestamp)
        val sendTimestamp = span.annotations.find(_.value == Constants.SERVER_SEND).map(_.timestamp)
        val (timestamp, duration) = (for {
          recv <- recvTimestamp
          send <- sendTimestamp
        } yield (recv / 1000L, (send - recv) / 1000L)).getOrElse((-1L, -1L))

        def serviceAnnotations(span: thrift.Span, prefix: String): ServiceInstance = {
          val hostIp: Option[Integer] = getBinaryAnnotationValue(span, prefix + "ServiceHostIp")
          ServiceInstance(
            getBinaryAnnotationValue(span, prefix + "ServiceName"),
            getBinaryAnnotationValue(span, prefix + "ServiceVersion"),
            hostIp.map(InetAddresses.fromInteger(_).getHostAddress)
          )
        }

        val fromService = serviceAnnotations(span, "from")
        val toService = serviceAnnotations(span, "to")

        val requestUri = getBinaryAnnotationValue(span, "requestUri").getOrElse("__none__")
        val callDepth =  20 - getBinaryAnnotationValue(span, "rpcsRemaining").getOrElse(-1)

        // Note: status can be String or Int
        // See https://github.com/webedx-spark/infra-services/blob/master/playcour/playcour/app/org/coursera/playcour/filters/TraceServerFilter.scala#L89
        val status: Any = getBinaryAnnotationValue(span, "status").getOrElse(-1)
        val statusInt = status match {
          case s: Int => s
          case s: String => Integer.parseInt(s)
          case _ => throw new IllegalArgumentException("Invalid status code type: " + status.getClass)
        }

        totalProcessed += 1

        if (totalReceived % 1000 == 0) {
          logger.info(s"""|Processing stats: $totalReceived received, $totalServerReceived server
                          |spans, $totalProcessed processed""".stripMargin)
        }

        RichSpan(
          timestamp,
          span.traceId.toString,
          spanId,
          parentSpanId,
          fromService.name,
          fromService.version,
          fromService.hostIp,
          toService.name,
          toService.version,
          toService.hostIp,
          requestUri,
          statusInt,
          duration,
          callDepth)
      }

      producer.send(serverSpans)

      Future(Unit)
    }


    val receiver = newKafkaSpanReceiver(process, keyDecoder = None, valueDecoder = StringDecoder)

    val closer = Closable.sequence(receiver)
    closeOnExit(closer)

    logger.info("running and ready")
    Await.all(receiver)
  }
}
