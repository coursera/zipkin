package org.coursera.zipkin

import com.twitter.finagle.Httpx
import com.twitter.finagle.param
import com.twitter.logging.Logger
import com.twitter.scrooge.BinaryThriftStructSerializer
import com.twitter.server.Closer
import com.twitter.server.TwitterServer
import com.twitter.util.Await
import com.twitter.util.Base64StringEncoder
import com.twitter.util.Future
import com.twitter.zipkin.cassandra.CassandraSpanStoreFactory
import com.twitter.zipkin.conversions.thrift._
import com.twitter.zipkin.query.ThriftQueryService
import com.twitter.zipkin.receiver.kafka.KafkaProcessor
import com.twitter.zipkin.thriftscala.{Span => ThriftSpan}
import com.twitter.zipkin.receiver.kafka.KafkaSpanReceiverFactory
import com.twitter.zipkin.web.ZipkinWebFactory

import scala.util.Try

object StringDecoder extends KafkaProcessor.KafkaDecoder {
  private[this] val deserializer = new BinaryThriftStructSerializer[ThriftSpan] {
    override val encoder = Base64StringEncoder
    val codec = ThriftSpan
  }

  override def fromBytes(bytes: Array[Byte]): List[ThriftSpan] = {
    val s = new String(bytes)
    s.split(",").flatMap(d => Try(deserializer.fromString(d)).toOption).toList
  }
}

object Main
  extends TwitterServer
  with CassandraSpanStoreFactory
  with KafkaSpanReceiverFactory
  with ZipkinWebFactory {

  def main() {
    val logger = Logger.get("Main")

    val store = newCassandraStore()

    def process(spans: Seq[ThriftSpan]): Future[Unit] = {
      val converted = spans.map(_.toSpan)
      store(converted) rescue {
        case t: Throwable =>
          logger.error("Error while writing span.", t)
          Future.value(())
      }
    }

    val kafkaReceiver = newKafkaSpanReceiver(process, valueDecoder = StringDecoder)

    val query = new ThriftQueryService(store)
    val server = Httpx.server
      .configured(param.Label("zipkin-web"))
      .serve(webServerPort(), newWebServer(query))

    onExit {
      server.close()
      kafkaReceiver.close()
      store.close()
    }

    Await.all(server, kafkaReceiver)
  }

}
