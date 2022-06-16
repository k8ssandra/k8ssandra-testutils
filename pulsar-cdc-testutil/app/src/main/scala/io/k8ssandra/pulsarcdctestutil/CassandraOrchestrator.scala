package io.k8ssandra.pulsarcdctestutil

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql._
import java.net.InetSocketAddress
import java.time.Duration

case class CassandraOrchestrator(cassandraContactPoints: String, cassAuthClass: String, cassAuthParms: String) {
  var sesssion: Option[CqlSession] = None
  def sessionOpen(dc: String): Either[Throwable, Unit] = {
    try {
      val session = CqlSession
        .builder
        .addContactPoint(
          InetSocketAddress.createUnresolved(
            cassandraContactPoints.split(":")(0),
            cassandraContactPoints.split(":")(1).toInt
          )
        )
        .withLocalDatacenter(dc)
        .build()
      this.sesssion = Some(session)
      Right(())

    }
    catch {
      case e: Exception => Left(e)
    }
  }
  def sessionClose(): Unit = {
    if (this.sesssion.nonEmpty) {
      this.sesssion.get.close()
    }
  }
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

  def addData(dc: String): Either[Throwable, Unit] = {
    if (this.sesssion.isEmpty) {
      val openRes = this.sessionOpen(dc)
      if (openRes.isLeft) { return Left(openRes.left.get) }
    }
    for (statement <- cqlStatements.split(";")) {
      try {
        while (!this.sesssion.get.checkSchemaAgreement()) {
          wait(2000) // 2 second wait
        }
        this.sesssion.get.execute(SimpleStatement.newInstance(statement + ";").setTimeout(Duration.ofSeconds(10)))
      }
      catch { case e: Exception => return Left(e) }
    }
    Right(())
  }

  def migrate(dc: String): Either[Throwable, Unit] = {
    val migrationStatements =
      s"""
        |DROP KEYSPACE IF EXISTS db1;
        |CREATE KEYSPACE db1 WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '${dc}':'1'};
        |CREATE TABLE IF NOT EXISTS db1.table1 (key text PRIMARY KEY, c1 text) WITH cdc=true;
        |""".stripMargin.strip()
    if (this.sesssion.isEmpty) {
      val openRes = this.sessionOpen(dc)
      if (openRes.isLeft) { return Left(openRes.left.get) }
    }
    for (statement <- migrationStatements.split(";")) {
      try {
        while (!this.sesssion.get.checkSchemaAgreement()) {
          wait(2000) // 2 second wait
        }
        this.sesssion.get.execute(SimpleStatement.newInstance(statement+ ";").setTimeout(Duration.ofSeconds(10)))
      }
      catch { case e: Exception => return Left(e) }
    }
    Right(())
  }
}
