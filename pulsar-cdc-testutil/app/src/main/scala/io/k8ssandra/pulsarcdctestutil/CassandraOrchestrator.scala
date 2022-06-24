package io.k8ssandra.pulsarcdctestutil

import com.datastax.oss.driver.api.core.cql._
import java.time.Duration

case class CassandraOrchestrator(cassandraClient: CassandraClients) {

  def addData(dc: String, dataStatements: String): Either[Throwable, Unit] = {
    if (cassandraClient.session.isEmpty) {
      val openRes = cassandraClient.sessionOpen(dc)
      if (openRes.isLeft) { return Left(openRes.left.get) }
    }
    for (statement <- dataStatements.split(";")) {
      try {
        while (!cassandraClient.session.get.checkSchemaAgreement()) {
          wait(2000) // 2 second wait
        }
        cassandraClient.session.get.execute(SimpleStatement.newInstance(statement + ";").setTimeout(Duration.ofSeconds(10)))
      }
      catch { case e: Exception => return Left(e) }
    }
    Right(())
  }

  def migrate(dc: String, migrationStatements: String): Either[Throwable, Unit] = {

    if (cassandraClient.session.isEmpty) {
      val openRes = cassandraClient.sessionOpen(dc)
      if (openRes.isLeft) { return Left(openRes.left.get) }
    }
    for (statement <- migrationStatements.split(";")) {
      try {
        while (!cassandraClient.session.get.checkSchemaAgreement()) {
          wait(2000) // 2 second wait
        }
        cassandraClient.session.get.execute(SimpleStatement.newInstance(statement+ ";").setTimeout(Duration.ofSeconds(10)))
      }
      catch { case e: Exception => return Left(e) }
    }
    Right(())
  }
}
