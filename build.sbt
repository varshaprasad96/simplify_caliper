name := "simplify_caliper"

version := "0.1"

scalaVersion := "2.11.12"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2",
  "org.apache.spark" %% "spark-sql" % "2.3.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.2.2",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.2.2",
  "mysql" % "mysql-connector-java" % "8.0.17",
  // https://mvnrepository.com/artifact/com.microsoft.azure/azure-data-lake-store-sdk
  "com.microsoft.azure" % "azure-data-lake-store-sdk" % "2.0.11"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}