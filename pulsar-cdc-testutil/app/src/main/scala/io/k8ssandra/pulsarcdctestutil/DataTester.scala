package io.k8ssandra.pulsarcdctestutil

import org.slf4j.Logger

object DataTester {
  def apply(cassandraOrchestrator: CassandraOrchestrator, pulsarOrchestrator: PulsarOrchestrator, logger: Logger): DataTester = {
    new DataTester(cassandraOrchestrator: CassandraOrchestrator, pulsarOrchestrator: PulsarOrchestrator, logger: Logger)
  }
}

class DataTester(cassandraOrchestrator: CassandraOrchestrator, pulsarOrchestrator: PulsarOrchestrator, logger: Logger) {
  val cqlStatements : String =
    """
      |INSERT INTO db1.table1 (key,c1) VALUES ('0','bob1');
      |INSERT INTO db1.table1 (key,c1) VALUES ('0','bob2'); INSERT INTO db1.table1 (key,c1) VALUES ('1','bob2');
      |DELETE FROM db1.table1 WHERE key='1';
      |ALTER TABLE db1.table1 ADD c2 int;
      |CREATE TYPE db1.t1 (a text, b text);
      |ALTER TABLE db1.table1 ADD c3 t1;
      |INSERT INTO db1.table1 (key,c1, c2, c3) VALUES ('3','bob', 1, {a:'a', b:'b'});
      |INSERT INTO db1.table1 (key,c1, c2, c3) VALUES ('4','bob', 1, {a:'a', b:'b'});
      |INSERT INTO db1.table1 (key,c1, c2, c3) VALUES ('5','bob', 1, {a:'a', b:'b'});
      |INSERT INTO db1.table1 (key,c1, c2, c3) VALUES ('6','bob', 1, {a:'a', b:'b'});
      |INSERT INTO db1.table1 (key,c1, c2, c3) VALUES ('7','bob', 1, {a:'a', b:'b'});
      |INSERT INTO db1.table1 (key,c1, c2, c3) VALUES ('8','bob', 1, {a:'a', b:'b'});
      |INSERT INTO db1.table1 (key,c1, c2, c3) VALUES ('9','bob', 1, {a:'a', b:'b'});
      |INSERT INTO db1.table1 (key,c1, c2, c3) VALUES ('10','bob', 1, {a:'a', b:'b'});
      |
      |""".stripMargin.strip()

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

  def run(cassDCName: String, PulsarCassContactPoints: String): Option[Throwable] = {
   // Create tables in Cassandra
   val migrationStatements =
     s"""
        |DROP KEYSPACE IF EXISTS db1;
        |CREATE KEYSPACE db1 WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '${cassDCName}':'1'};
        |CREATE TABLE IF NOT EXISTS db1.table1 (key text PRIMARY KEY, c1 text) WITH cdc=true;
        |""".stripMargin.strip()

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
     res.left.map(m => logger.error(s"Data fetch on topic failed: ${m.getMessage}"))
     return res.left.toOption
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
