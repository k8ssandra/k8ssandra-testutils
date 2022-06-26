package io.k8ssandra.pulsarcdctestutil

object StaticData {
  def apply(cassDCName: String): StaticData = {
    new StaticData(cassDCName)
  }
}

class StaticData(cassDCName: String) {
  val migrationStatements: String =
    s"""
       |DROP KEYSPACE IF EXISTS db1;
       |CREATE KEYSPACE db1 WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '$cassDCName':'1'};
       |CREATE TABLE IF NOT EXISTS db1.table1 (key text PRIMARY KEY, c1 text) WITH cdc=true;
       |""".stripMargin.strip()

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

}
