package io.k8ssandra.pulsarcdctestutil


import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException
import org.apache.pulsar.common.schema.{KeyValue, KeyValueEncodingType}
import org.apache.pulsar.common.io.SourceConfig
import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.jdk.CollectionConverters._
import scala.util.Try
import com.sksamuel.pulsar4s.{ConsumerConfig, MessageId, PulsarClient, Subscription, Topic}

object PulsarOrchestrator {
  def apply(pulsarClients: PulsarClients): PulsarOrchestrator = {
    new PulsarOrchestrator(pulsarClients: PulsarClients)
  }
}

class PulsarOrchestrator(pulsarClients: PulsarClients) {
  def connectorConfigure(cassDC: String,
                         cassContactPoint: String,
                         keyspace: String,
                         table: String): Either[Throwable, Unit] = {
    if (pulsarClients.adminClient.isFailure) {
      return Left(pulsarClients.adminClient.toEither.left.get)
    }
    val delResult = Try {
      pulsarClients.adminClient.get.sources().deleteSource(
        "public",
        "default",
        "cassandra-source-db1-table1"
      )
    }
    if (delResult.isFailure && !delResult.failed.get.isInstanceOf[NotFoundException]) {
      return delResult.toEither
    }

    val sourceConfig = SourceConfig
      .builder()
      .tenant("public")
      .namespace("default")
      .name("cassandra-source-db1-table1")
      .topicName("data-db1.table1")
      .configs(
        Map[String, AnyRef](
          "keyspace" -> keyspace,
          "table" -> table,
          "events.topic" -> "persistent://public/default/events-db1.table1", // TODO: This should be tunable.
          "events.subscription.name" -> "sub1",
          "contactPoints" -> cassContactPoint,
          "loadBalancing.localDc" -> cassDC,
          "auth.provider" -> "None" // TODO: Implement Cassandra auth.
        ).asJava
      ).archive("builtin://cassandra-source").build()
    try {
      pulsarClients.adminClient.get
        .sources()
        .createSource(sourceConfig, sourceConfig.getArchive)
    } catch {
      case e: Exception => return Left(e)
    }
      Right(())
  }

  def fetchData(): Either[Throwable, Set[(db1.table1key, db1.table1value)]] = {
    if (pulsarClients.consumer.isFailure) {
      return Left(pulsarClients.adminClient.toEither.left.get)
    }
    pulsarClients.consumer.get.seek(MessageId.earliest)
    var messageList: Set[(db1.table1key, db1.table1value)] = Set.empty
    while (true) {
        val message = pulsarClients.consumer.get.receive(duration = FiniteDuration(30, SECONDS))
        if (message.isFailure) {
          return Left(new Error("Error retrieving event from Pulsar"))
        } else if (message.get.isEmpty) {
          return Right(messageList)
        } else {
          val innerMessage = message.get.get.valueTry
          if (innerMessage.isFailure) {
            return Left(new Error("Error deserialising event from Pulsar"))
          } else {
            messageList = messageList + ((innerMessage.get.getKey, innerMessage.get.getValue))
          }
        }
    }
    Right(messageList)
  }
}
