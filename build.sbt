name := "simplify_caliper"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2",
  "org.apache.spark" %% "spark-sql" % "2.3.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.2.2",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.2.2",
  "mysql" % "mysql-connector-java" % "5.1.46"  // need 5.1.45 for azure mysql; 46 for local mac
)