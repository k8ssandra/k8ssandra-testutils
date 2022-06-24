package io.k8ssandra.pulsarcdctestutil

import com.datastax.oss.driver.api.core.CqlSession

import java.net.InetSocketAddress

trait CassandraClients {
  var session: Option[CqlSession]
  def sessionOpen(dc: String): Either[Throwable, Unit]
  def sessionClose(): Unit
}

object CassandraDefaultClients {
  def apply(cassandraContactPoints: String): CassandraDefaultClients = {
    new CassandraDefaultClients(cassandraContactPoints)
  }
}

class CassandraDefaultClients(cassandraContactPoints: String) extends CassandraClients {
  var session: Option[CqlSession] = None
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
      this.session = Some(session)
      Right(())
    }
    catch {
      case e: Exception => Left(e)
    }
  }
  def sessionClose(): Unit = {
    if (this.session.nonEmpty) {
      this.session.get.close()
    }
  }

}