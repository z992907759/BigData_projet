ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.17"
ThisBuild / scalacOptions ++= Seq("-target:jvm-11")
ThisBuild / javacOptions  ++= Seq("--release", "11")

lazy val root = (project in file("."))
  .settings(
    name := "ex01_data_retrieval",
    libraryDependencies ++= Seq(
      "org.apache.spark"  %% "spark-core" % "3.5.5",
      "org.apache.spark"  %% "spark-sql"  % "3.5.5",
      "org.apache.hadoop" %  "hadoop-aws" % "3.3.4",
      "com.amazonaws"     %  "aws-java-sdk-bundle" % "1.12.262"
    ),

    // Java 11 (LTS) for the course: compile/target JVM 11
    run / fork := true
  )