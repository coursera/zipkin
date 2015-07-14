package org.coursera.zipkin.courrelate

import java.util.Properties

import com.twitter.logging.Logger
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import play.api.libs.json.Json

import scala.collection.JavaConverters._

class CourrelateProducer {
  val logger = Logger.get(classOf[CourrelateProducer])

  import Formats._

  private[this] val producer = {
    val downstreamKafkaBrokerList = System.getProperty("downstreamKafka.metadata.broker.list")

    val props = new Properties()
    props.put("metadata.broker.list", downstreamKafkaBrokerList)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    props.put("producer.type", "async")
    props.put("compression.codec", "gzip")

    val producerConfig = new ProducerConfig(props)
    new Producer[String, String](producerConfig)
  }

  def send(spans: Seq[RichSpan]): Unit = {
    val messages = spans.map { span =>
      val jsonString = Json.stringify(Json.toJson(span))
      logger.ifDebug(s"Sending JSON to Kafka: $jsonString")

      new KeyedMessage[String, String]("courrelate", span.spanId.toString, jsonString)
    }

    producer.send(messages.toList.asJava)
  }
}
