package org.coursera.zipkin.courrelate

import com.twitter.zipkin.{thriftscala => thrift}
import com.twitter.zipkin.thriftscala.AnnotationType
import com.twitter.zipkin.thriftscala.BinaryAnnotation

object SpanUtils {
  def printSpan(span: thrift.Span): String = {
    val annotations = span.annotations.map( a =>
      s"(ts=${a.timestamp}, host=${a.host}, dur=${a.duration}, val=${a.value})").mkString(",")
    val binaryAnnotations = span.binaryAnnotations.map { a =>

      val valueString = getBinaryAnnotationValue(a).toString
      s"(type=${a.annotationType}, host=${a.host}, key=${a.key}, val=$valueString)"
    }.mkString(",\n  ")

    s"""START SPAN ===
       |Id: ${span.id}
        |TraceId: ${span.traceId}
        |ParentId: ${span.parentId}
        |Annotations: $annotations
        |Binary Annotations: $binaryAnnotations
        |Name: ${span.name}
        |=== END SPAN\n
     """.stripMargin
  }

  def getServiceName(span: thrift.Span): Option[String] = {
    val serviceName =
      if (span != null) {
        span.binaryAnnotations.find(_.key == "requestUri").map(_.host.map(_.serviceName))
      } else {
        None
      }
    serviceName.flatten
  }

  def getBinaryAnnotationValue[T](span: thrift.Span, key: String): Option[T] = {
    span.binaryAnnotations.find(_.key == key).map { ann =>
      getBinaryAnnotationValue(ann).asInstanceOf[T]
    }
  }

  def getBinaryAnnotationValue[T](ann: BinaryAnnotation): Any = {
    val result = ann.annotationType match {
      case AnnotationType.String =>
        val buffer = ann.value
        val arr = new Array[Byte](buffer.remaining)
        buffer.get(arr)
        new String(arr, "UTF-8")
      case AnnotationType.I32 => ann.value.asIntBuffer().get()
      case AnnotationType.I64 => ann.value.asLongBuffer().get()
      case _ => throw new IllegalArgumentException("Unhandled AnnotationType: " + ann.annotationType)
    }

    ann.value.rewind()

    result
  }
}
