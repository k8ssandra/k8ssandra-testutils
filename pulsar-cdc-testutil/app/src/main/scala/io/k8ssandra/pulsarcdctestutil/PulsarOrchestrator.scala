package io.k8ssandra.pulsarcdctestutil

import com.sksamuel.pulsar4s.{ConsumerConfig, MessageId, PulsarClient, Subscription, Topic}
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.common.schema.{KeyValue, KeyValueEncodingType}
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException
import org.apache.pulsar.common.io.SourceConfig

import scala.jdk.CollectionConverters._
import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.util.{Failure, Try}

object PulsarOrchestrator {
  def apply(pulsarURL: String,
            schemaRegistryURL: String,
            pulsarAdminURL: String,
            pulsarTopic: String,
            pulsarAuthClass: String,
            pulsarAuthParms: String): PulsarOrchestrator = {
    new PulsarOrchestrator(pulsarURL: String,
      schemaRegistryURL: String,
      pulsarAdminURL: String,
      pulsarTopic: String,
      pulsarAuthClass: String,
      pulsarAuthParms: String)
  }
}

class PulsarOrchestrator(pulsarURL: String,
                         schemaRegistryURL: String,
                         pulsarAdminURL: String,
                         pulsarTopic: String,
                         pulsarAuthClass: String,
                         pulsarAuthParms: String) {

  private val client = PulsarClient(pulsarURL)
  private val adminClient = PulsarAdmin
    .builder()
    .serviceHttpUrl(pulsarAdminURL)
    // TODO enable auth, TLS.
    .build()
  implicit val schema: Schema[KeyValue[db1.table1key, db1.table1value]] =
    Schema.KeyValue(
      Schema.AVRO(classOf[db1.table1key]),
      Schema.AVRO(classOf[db1.table1value]),
      KeyValueEncodingType.SEPARATED
    )
  private val consumer = client.consumer(
    ConsumerConfig(Subscription("mysubs"),
      List(Topic(pulsarTopic))
    )
  )

  def connectorConfigure(cassDC: String,
                         cassContactPoint: String,
                         keyspace: String,
                         table: String): Either[Throwable, Unit] = {

    val delResult = Try {
      adminClient.sources().deleteSource(
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
      adminClient
        .sources()
        .createSource(sourceConfig, sourceConfig.getArchive)
    } catch {
      case e: Exception => return Left(e)
    }
      Right(())
  }

  val expectedData: Set[(db1.table1key, db1.table1value)] = Set[(db1.table1key, db1.table1value)](
    (db1.table1key.newBuilder.setKey("9").build(), db1.table1value.newBuilder().setC1("bob").setC2(1).setC3(db1.t1.newBuilder().setA("a").setB("b").build()).build()),
    (db1.table1key.newBuilder.setKey("1").build(), null),
    (db1.table1key.newBuilder.setKey("10").build(), db1.table1value.newBuilder().setC1("bob").setC2(1).setC3(db1.t1.newBuilder().setA("a").setB("b").build()).build()),
    (db1.table1key.newBuilder.setKey("7").build(), db1.table1value.newBuilder().setC1("bob").setC2(1).setC3(db1.t1.newBuilder().setA("a").setB("b").build()).build()),
    (db1.table1key.newBuilder.setKey("0").build(), db1.table1value.newBuilder().setC1("bob2").setC2(null).setC3(null).build()),
    (db1.table1key.newBuilder.setKey("5").build(), db1.table1value.newBuilder().setC1("bob").setC2(1).setC3(db1.t1.newBuilder().setA("a").setB("b").build()).build()),
    (db1.table1key.newBuilder.setKey("8").build(), db1.table1value.newBuilder().setC1("bob").setC2(1).setC3(db1.t1.newBuilder().setA("a").setB("b").build()).build()),
    (db1.table1key.newBuilder.setKey("3").build(), db1.table1value.newBuilder().setC1("bob").setC2(1).setC3(db1.t1.newBuilder().setA("a").setB("b").build()).build()),
    (db1.table1key.newBuilder.setKey("6").build(), db1.table1value.newBuilder().setC1("bob").setC2(1).setC3(db1.t1.newBuilder().setA("a").setB("b").build()).build()),
    (db1.table1key.newBuilder.setKey("4").build(), db1.table1value.newBuilder().setC1("bob").setC2(1).setC3(db1.t1.newBuilder().setA("a").setB("b").build()).build()),
  )

  def checkData(data: Set[(db1.table1key, db1.table1value)]): Either[Throwable, Unit] = {
    if (expectedData != data) {
      return Right(())
    }
    Left(new Error("Data did not match expected values."))
  }

  def fetchData(): Either[Throwable, Set[(db1.table1key, db1.table1value)]] = {
    consumer.seek(MessageId.earliest)
    var messageList: Set[(db1.table1key, db1.table1value)] = Set.empty
    while (true) {
        val message = consumer.receive(duration = FiniteDuration(30, SECONDS))
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
