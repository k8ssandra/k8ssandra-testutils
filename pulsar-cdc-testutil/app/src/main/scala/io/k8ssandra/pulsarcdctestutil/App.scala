package io.k8ssandra.pulsarcdctestutil

import com.sksamuel.pulsar4s._
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.common.schema.{KeyValue, KeyValueEncodingType}

import util.control.Breaks._


object App {
  def main(args: Array[String]): Unit = {
    printPulsar()
  }

  def addToCassandra(): Unit = {

  }

  def printPulsar(): Unit = {
    val client = PulsarClient("pulsar://localhost:11000")
    val topic = Topic("persistent://public/default/data-db1.table1")
    implicit val schema: Schema[KeyValue[db1.table1key, db1.table1value]] =
      Schema.KeyValue(
        Schema.AVRO(classOf[db1.table1key]),
        Schema.AVRO(classOf[db1.table1value]),
        KeyValueEncodingType.SEPARATED
      )
    val consumer = client.consumer(ConsumerConfig(Subscription("mysubs"), List(topic)))
    consumer.seek(MessageId.earliest)
    while (true) {
      breakable {
        val message = consumer.receive
        if (message.isFailure) {
          println(s"ConsumerMessage failed, failure was ${message.failed.get}")
          break
        } else  {
          val innerMessage = message.get.valueTry
          if (innerMessage.isFailure) {
            println(s"innerMessage failed, failure was ${innerMessage.failed.get}")
            break
          } else {
            println(s"innerMessage succeeded, values were ${innerMessage.get.getKey}: ${innerMessage.get.getValue}")
          }
        }
      }
    }
  }
}