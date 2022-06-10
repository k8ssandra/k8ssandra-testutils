# K8ssandra test utilities

## pulsar-cdc-testutils

Written in: Scala.
Components: Cassandra, Pulsar, Kubernetes.

### Description
This is a utility that aims to:

* Create a Cassandra schema.
* Create a Pulsar connector to listen to CDC changes on the table created.
* Add data to Cassandra.
* Listen to the Pulsar topic to ensure that events arrive as expected.

You can use the source to learn:

* How to work with the Pulsar admin API to configure connectors (currently this isn't documented anywhere).
* How to read data from a Pulsar topic.
* How to write data into Cassandra using the DataStax core drivers for Apache Cassandra.

The utility assumes the presence of Kubernetes, Cassandra and Pulsar clusters, you can learn how to configure these components by examining [this](https://github.com/k8ssandra/cass-operator/tree/main/test/kuttl/test-cdc) kuttl test. 