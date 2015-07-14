package org.coursera.zipkin.courrelate

import play.api.libs.json.Json

case class ServiceInstance(
  name: Option[String],
  version: Option[String],
  hostIp: Option[String])

case class RichSpan(
  timestamp: Long,
  traceId: String,
  spanId: Long,
  parentSpanId: Option[Long],
  fromServiceName: Option[String],
  fromServiceVersion: Option[String],
  fromServiceHostIp: Option[String],
  toServiceName: Option[String],
  toServiceVersion: Option[String],
  toServiceHostIp: Option[String],
  uri: String,
  statusCode: Int,
  duration: Long,
  callDepth: Int)

object Formats {
  implicit val serviceInstanceFmt = Json.format[ServiceInstance]
  implicit val richSpanFmt = Json.format[RichSpan]
}