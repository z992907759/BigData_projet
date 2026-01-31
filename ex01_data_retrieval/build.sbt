ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "ex01_data_retrieval",
    libraryDependencies ++= Seq(
      "org.apache.spark"  %% "spark-core" % "3.5.5",
      "org.apache.spark"  %% "spark-sql"  % "3.5.5",
      "org.apache.hadoop" %  "hadoop-aws" % "3.3.4",
      "com.amazonaws"     %  "aws-java-sdk-bundle" % "1.12.262"
    ),

    // Java 17 + Spark: make sure JVM options are actually applied
    run / fork := true,
    run / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
  )