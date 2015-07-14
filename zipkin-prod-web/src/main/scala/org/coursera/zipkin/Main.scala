package org.coursera.zipkin

import com.twitter.logging.Logger
import com.twitter.zipkin.cassandra.CassieSpanStoreFactory
import com.twitter.finagle.Http
import com.twitter.server.{Closer, TwitterServer}
import com.twitter.util.{Await, Closable}
import com.twitter.zipkin.web.ZipkinWebFactory
import com.twitter.zipkin.query.ThriftQueryService
import com.twitter.zipkin.query.constants.DefaultAdjusters


object Main extends TwitterServer with Closer
  with ZipkinWebFactory
  with CassieSpanStoreFactory
{
  def main() {
    val logger = Logger.get("Main")
    val store = newCassandraStore()

    val query = new ThriftQueryService(store, adjusters = DefaultAdjusters)
    val webService = newWebServer(query, statsReceiver.scope("web"))
    val web = Http.serve(webServerPort(), webService)

    val closer = Closable.sequence(web, store)
    closeOnExit(closer)

    println("running and ready")
    Await.all(web, store)
  }
}
