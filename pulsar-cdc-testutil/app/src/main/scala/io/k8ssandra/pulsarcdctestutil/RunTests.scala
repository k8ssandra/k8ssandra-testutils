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

  lazy val cassClient = CassandraDefaultClients(cassandraContactPoints)
  lazy val cassOrchestrator = CassandraOrchestrator(cassClient)
  lazy val pulsarClients = new PulsarClients(pulsarURL: String, pulsarAdminURL: String, pulsarTopic: String)
  lazy val pulsarOrchestrator = PulsarOrchestrator(pulsarClients)

  val logger: Logger = LoggerFactory.getLogger(classOf[RunTests])
  val staticData = StaticData(cassDCName)

  @Command(name = "create-schema")
  def createSchema(): Int = {
    logger.info(s"Creating Cassandra schema, dc is ${cassDCName}.")
    var res : Either[Throwable, _] = cassOrchestrator.migrate(cassDCName, staticData.migrationStatements)
    if (res.isLeft) {
      res.left.map(m => logger.error(s"Failed to create Cassandra schema: ${m.getMessage}"))
      return ExitCode.SOFTWARE
    }
    return ExitCode.OK
  }
  @Command(name = "create-connector")
  def createConnector(): Unit = {
    val res = pulsarOrchestrator.connectorConfigure(
      cassDC=cassDCName,
      cassContactPoint=PulsarCassContactPoints,
      keyspace="db1",
      table="table1"
    )
    if (res.isLeft) {
      res.left.map(m => logger.error(s"Failed to configure Pulsar connector: ${m.getMessage}"))
      return ExitCode.SOFTWARE
    }
  }
  @Command(name = "cassandra-insert")
  def cassandraInsert(): Int = {
    logger.info("Inserting data into Cassandra.")
    val res = cassOrchestrator.addData(cassDCName, staticData.cqlStatements)
    if (res.isLeft) {
      res.left.map(m => logger.error(s"Failed to add data to Cassandra: ${m.getMessage}"))
      return ExitCode.SOFTWARE
    }
    return ExitCode.OK
  }

  @Command(name = "pulsar-fetch")
  def pulsarFetch(): Int = {
    logger.info("Fetching data from Pulsar.")
    // Fetch data from Pulsar
    val maybeData = pulsarOrchestrator.fetchData()
    if (maybeData.isLeft) {
      maybeData.left.map(m => logger.error(s"Data fetch on topic failed: ${m.getMessage}"))
      return ExitCode.SOFTWARE
    }
    maybeData.map(d => logger.info(s"Data: ${d.toString()}"))
    ExitCode.OK
  }

  override def call(): Int = {
    logger.info("Starting application.")
    val tester = DataTester(cassOrchestrator, pulsarOrchestrator, logger)
    if (tester.run(cassDCName, PulsarCassContactPoints, staticData.migrationStatements, staticData.cqlStatements).nonEmpty) {
      logger.error(s"Test failed ")
      return ExitCode.SOFTWARE
    } else {
      logger.info("SUCCESS")
      return ExitCode.OK
    }
  }
}