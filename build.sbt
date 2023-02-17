ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

organization      := "org.vigil"

lazy val root = (project in file("."))
  .settings(
    name := "vgl-scala-test"
  )

  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.3.1",
    "org.apache.spark" %% "spark-sql" % "3.3.1",
    "org.scalatest" %% "scalatest" % "3.2.11" % Test,
    "org.scalatest" %% "scalatest-funsuite" % "3.3.0-SNAP2" % Test
  )
