name := "kafka-streams-intro"

version := "1.0.0"

scalaVersion := Version.Scala

libraryDependencies ++= Seq(
  "io.confluent" % "kafka-avro-serializer" % Version.Confluent,
  "io.confluent" % "kafka-streams-avro-serde" % Version.Confluent,
//  "io.confluent" % "kafka-schema-registry" % Version.Confluent,
  "org.scalatest" %% "scalatest" % Version.ScalaTest % "test",
  "org.clapper" %% "grizzled-slf4j" % Version.GrizzledSlf4J,
//  "io.dropwizard.metrics" % "metrics-core" % Version.DropwizardMetrics,
//  "io.dropwizard.metrics" % "metrics-jvm" % Version.DropwizardMetrics,
//  "org.coursera" % "metrics-datadog" % Version.DatadogMetrics,
//  "org.mpierce.metrics.reservoir" % "hdrhistogram-metrics-reservoir" % Version.HdrhistogramMetrics,
//  "com.github.scopt" %% "scopt" % Version.Scopt,
  "org.apache.kafka" %% "kafka" % Version.Kafka,
  "org.apache.kafka" % "kafka-streams" % Version.Kafka,
  "org.apache.kafka" %% "kafka-streams-scala" % Version.Kafka,
  "org.apache.kafka" % "kafka-clients" % Version.Kafka // needed for producer in BankBalance

)

resolvers ++= Seq(
  Resolver.mavenLocal,
  "Confluent" at "https://packages.confluent.io/maven/"
)