package com.twitter.mycollector

import com.twitter.scrooge.BinaryThriftStructSerializer
import com.twitter.util.Base64StringEncoder
import com.twitter.zipkin.cassandra.CassieSpanStoreFactory
import com.twitter.zipkin.conversions.thrift._
import com.twitter.finagle.Http
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.server.{Closer, TwitterServer}
import com.twitter.util.{Await, Closable, Future}
import com.twitter.zipkin.anormdb.AnormDBSpanStoreFactory
import com.twitter.zipkin.collector.SpanReceiver
import com.twitter.zipkin.common.Span
import com.twitter.zipkin.receiver.kafka.KafkaProcessor
import com.twitter.zipkin.receiver.kafka.KafkaSpanReceiverFactory
import com.twitter.zipkin.{thriftscala => thrift}
import com.twitter.zipkin.receiver.scribe.ScribeSpanReceiverFactory
import com.twitter.zipkin.zookeeper.ZooKeeperClientFactory
import com.twitter.zipkin.web.ZipkinWebFactory
import com.twitter.zipkin.query.ThriftQueryService
import com.twitter.zipkin.query.constants.DefaultAdjusters
import com.twitter.zipkin.tracegen.ZipkinSpanGenerator

object StringDecoder extends KafkaProcessor.KafkaDecoder {
  private[this] val deserializer = new BinaryThriftStructSerializer[thrift.Span] {
    override val encoder = Base64StringEncoder
    val codec = thrift.Span
  }

  override def fromBytes(
    bytes: Array[Byte]): Option[List[thrift.Span]] = {

    val s = new String(bytes)
    Some(s.split(",").map(deserializer.fromString).toList)
  }
}

object Main extends TwitterServer with Closer
  with KafkaSpanReceiverFactory
  with ZipkinWebFactory
  with CassieSpanStoreFactory
  with ZipkinSpanGenerator
{
  val genSampleTraces = flag("genSampleTraces", false, "Generate sample traces")

  def main() {
    val store = newCassandraStore()
    if (genSampleTraces())
      Await.result(generateTraces(store))

    val convert: Seq[thrift.Span] => Seq[Span] = { x => println(x); x.map(_.toSpan) }
    val receiver = newKafkaSpanReceiver(convert andThen store, keyDecoder = None, valueDecoder = StringDecoder)
    val query = new ThriftQueryService(store, adjusters = DefaultAdjusters)
    val webService = newWebServer(query, statsReceiver.scope("web"))
    val web = Http.serve(webServerPort(), webService)

    val closer = Closable.sequence(web, receiver, store)
    closeOnExit(closer)

    println("running and ready")
    Await.all(web, receiver, store)
  }
}
