package org.coursera.zipkin

import com.twitter.logging.Logger
import com.twitter.scrooge.BinaryThriftStructSerializer
import com.twitter.util.Base64StringEncoder
import com.twitter.zipkin.cassandra.CassieSpanStoreFactory
import com.twitter.zipkin.conversions.thrift._
import com.twitter.finagle.Http
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.server.{Closer, TwitterServer}
import com.twitter.util.{Await, Closable, Future}
import com.twitter.zipkin.collector.SpanReceiver
import com.twitter.zipkin.common.Span
import com.twitter.zipkin.receiver.kafka.KafkaProcessor
import com.twitter.zipkin.receiver.kafka.KafkaSpanReceiverFactory
import com.twitter.zipkin.{thriftscala => thrift}
import com.twitter.zipkin.zookeeper.ZooKeeperClientFactory
import com.twitter.zipkin.web.ZipkinWebFactory
import com.twitter.zipkin.query.ThriftQueryService
import com.twitter.zipkin.query.constants.DefaultAdjusters
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

object Main extends TwitterServer with Closer
  with KafkaSpanReceiverFactory
  with ZipkinWebFactory
  with CassieSpanStoreFactory
{
  def main() {
    val logger = Logger.get("Main")
    val store = newCassandraStore()

    def process(spans: Seq[thrift.Span]): Future[Unit] = {
      val converted = spans.map(_.toSpan)
      store(converted) rescue {
        case t: Throwable =>
          logger.error("Error while writing span.", t)
          Future.value(())
      }
    }

    val receiver = newKafkaSpanReceiver(process, keyDecoder = None, valueDecoder = StringDecoder)
    val query = new ThriftQueryService(store, adjusters = DefaultAdjusters)
    val webService = newWebServer(query, statsReceiver.scope("web"))
    val web = Http.serve(webServerPort(), webService)

    val closer = Closable.sequence(web, receiver, store)
    closeOnExit(closer)

    println("running and ready")
    Await.all(web, receiver, store)
  }
}
