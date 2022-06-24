package io.k8ssandra.pulsarcdctestutil


import picocli.CommandLine._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.Callable

@Command(
  name = "run-tests",
  mixinStandardHelpOptions = true,
  version = Array("checksum 4.0"),
  description = Array(
    "Configures Pulsar Cassandra connector, adds tables + data to Cassandra, checks Pulsar topic to ensure data has appeared."
  )
)
class RunTests extends Callable[Int] {
  @Option(
    names = Array("--pulsar-auth-class"),
    description = Array("Pulsar Auth plugin class.")
  )
  private var pulsarAuthClass = ""
  @Option(
    names = Array("--pulsar-auth-params"),
    description = Array("comma delimited Pulsar auth params.")
  )
  private var pulsarAuthParms = ""

  @Option(
    names = Array("--cass-contact-points"),
    description = Array("Cassandra contact point.")
  )
  private var cassandraContactPoints = "test-cluster-dc1-all-pods-service.cass-operator.svc.cluster.local:9042"

  @Option(
    names = Array("--cass-dc"),
    description = Array("Cassandra datacenter name.")
  )
  private var cassDCName = "dc1"

  @Option(
    names = Array("--cass-auth-class"),
    description = Array("Cassandra auth plugin class.")
  )
  private var cassAuthClass = ""

  @Option(
    names = Array("--cass-auth-parms"),
    description = Array("comma delimited Cassandra auth params.")
  )
  private var cassAuthParms = ""

  @Option(
    names = Array("--pulsar-url"),
    description = Array("Pulsar URL.")
  )
  private var pulsarURL = "pulsar://pulsar-proxy.pulsar.svc.cluster.local:6650"

  @Option(
    names = Array("--schema-registry-url"),
    description = Array("Schema Registry URL.")
  )
  private var schemaRegistryURL = "pulsar://pulsar-proxy.pulsar.svc.cluster.local:8080"

  @Option(
    names = Array("--pulsar-admin-url"),
    description = Array("Pulsar admin URL.")
  )
  private var pulsarAdminURL = "http://pulsar-proxy.pulsar.svc.cluster.local:8080"

  @Option(
    names = Array("--pulsar-topic"),
    description = Array("Pulsar topic.")
  )
  private var pulsarTopic ="persistent://public/default/data-db1.table1"

  @Option(
    names = Array("--pulsar-cass-contact-point"),
    description = Array("Pulsar's Cassandra contact points (as opposed to this application's.")
  ) // This is needed in case the contact points available to Pulsar aren't the same as those available to this application.
  private var PulsarCassContactPoints = cassandraContactPoints

  val logger: Logger = LoggerFactory.getLogger(classOf[RunTests])

  override def call(): Int = {
    logger.info("Starting application.")
    val cassClient = CassandraDefaultClients(cassandraContactPoints)
    val cassOrchestrator = CassandraOrchestrator(cassClient)
    val pulsarClients = new PulsarClients(pulsarURL: String, pulsarAdminURL: String, pulsarTopic: String)
    val pulsarOrchestrator = PulsarOrchestrator(pulsarClients)
    val tester = DataTester(cassOrchestrator, pulsarOrchestrator, logger)

    if (tester.run(cassDCName, PulsarCassContactPoints).nonEmpty) {
      logger.error(s"Test failed ")
      return ExitCode.SOFTWARE
    } else {
      logger.info("SUCCESS")
      return ExitCode.OK
    }
  }
}