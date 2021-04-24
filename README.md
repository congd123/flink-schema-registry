# Apache Flink using Schema registry

Example using Apache Flink and a schema registry to produce and consume events.

It was created two jobs:
1) Job that only consumes one type of events
2) Job that consumes multiple event types in a single topic

## Project Structure

The project was developed using:
- Apache Flink 1.12.1
- Scala 2.12
- Maven

## Schema registry plugin
The plugin has a set of [goals](https://docs.confluent.io/platform/current/schema-registry/develop/maven-plugin.html).

For instance the goal [schema-registry:register](https://docs.confluent.io/platform/current/schema-registry/develop/maven-plugin.html#schema-registry-register) is used to register our schemas in the registry.

## Generate application binary

To generate the application binary you can execute:
```console
mvn clean package
```

and in be created the artifacts of the job in the folder `/target` that can be deployed in a Flink Cluster.




