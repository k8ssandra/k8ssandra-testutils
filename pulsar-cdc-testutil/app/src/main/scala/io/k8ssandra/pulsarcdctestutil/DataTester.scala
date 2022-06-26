package io.k8ssandra.pulsarcdctestutil

import org.slf4j.Logger

object DataTester {
  def apply(cassandraOrchestrator: CassandraOrchestrator, pulsarOrchestrator: PulsarOrchestrator, logger: Logger): DataTester = {
    new DataTester(cassandraOrchestrator: CassandraOrchestrator, pulsarOrchestrator: PulsarOrchestrator, logger: Logger)
  }
}

class DataTester(cassandraOrchestrator: CassandraOrchestrator, pulsarOrchestrator: PulsarOrchestrator, logger: Logger) {

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
    logger.info(s"Expected data is \n ${expectedData}")
    logger.info(s"Actual data is \n ${data}")
    if (expectedData.hashCode() == data.hashCode() && expectedData.size == data.size) {
      return Right(())
    }
    return Left(new Error("Data did not match expected values."))
  }

  def run(cassDCName: String, PulsarCassContactPoints: String, migrationStatements: String, cqlStatements: String): Option[Throwable] = {
    // Create tables in Cassandra
    var res : Either[Throwable, _] = cassandraOrchestrator.migrate(cassDCName, migrationStatements)
    if (res.isLeft) {
      res.left.map(m => logger.error(s"Failed to create Cassandra schema: ${m.getMessage}"))
      return res.left.toOption
    }

   // Configure Pulsar connector.
    res = pulsarOrchestrator.connectorConfigure(
      cassDC=cassDCName,
      cassContactPoint=PulsarCassContactPoints,
      keyspace="db1",
      table="table1"
    )
    if (res.isLeft) {
      res.left.map(m => logger.error(s"Failed to configure Pulsar connector: ${m.getMessage}"))
      return res.left.toOption
    }

   // Add data to Cassandra
    res = cassandraOrchestrator.addData(cassDCName, cqlStatements)
    if (res.isLeft) {
      res.left.map(m => logger.error(s"Failed to add data to Cassandra: ${m.getMessage}"))
      return res.left.toOption
    }

    // Fetch data from Pulsar
    val maybeData = pulsarOrchestrator.fetchData()
    if (maybeData.isLeft) {
      maybeData.left.map(m => logger.error(s"Data fetch on topic failed: ${m.getMessage}"))
      return maybeData.left.toOption
    }

    val data = maybeData.right.get

    logger.info(s"Received data, length ${data.size}")
    logger.info(data.toString())

    // Check data from Pulsar
    res = checkData(data)
    if (res.isLeft) {
      res.left.map(m => logger.error(s"Data validation on topic failed: ${m.getMessage}"))
      return res.left.toOption
    }
    logger.info("SUCCESS")
    return None
 }
}
