package io.k8ssandra.pulsarcdctestutil

import com.sksamuel.pulsar4s.{ConsumerConfig, PulsarClient, Subscription, Topic}
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.common.schema.{KeyValue, KeyValueEncodingType}

object PulsarClients {
  def apply(pulsarURL: String, pulsarAdminURL: String, pulsarTopic: String): PulsarClients = {
    new PulsarClients(pulsarURL: String, pulsarAdminURL: String, pulsarTopic: String)
  }
}
// PulsarClients knows how to create Pulsar client connections.
class PulsarClients(pulsarURL: String, pulsarAdminURL: String, pulsarTopic: String) {
  val client = PulsarClient(pulsarURL)
  val adminClient = util.Try{
    PulsarAdmin // TODO enable auth, TLS.
      .builder()
      .serviceHttpUrl(pulsarAdminURL)
      .build()
  }
  implicit val schema: Schema[KeyValue[db1.table1key, db1.table1value]] =
    Schema.KeyValue(
      Schema.AVRO(classOf[db1.table1key]),
      Schema.AVRO(classOf[db1.table1value]),
      KeyValueEncodingType.SEPARATED
    )
  val consumer = util.Try {
    client.consumer(
      ConsumerConfig(Subscription("mysubs"),
        List(Topic(pulsarTopic))
      )
    )
  }
}
