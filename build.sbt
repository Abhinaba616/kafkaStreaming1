libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1"
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.7.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.1"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.7.0"
libraryDependencies += "io.delta" %% "delta-core" % "0.8.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}