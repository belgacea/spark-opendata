import ReleaseTransformations._

// Build

organization := "org.example"
name := "spark-opendata"
description := "Open Data Assignment"
developers := List(
  Developer(
    id    = "belgacea",
    name  = "Adam Belgacem",
    email = "adam.belgacem.com",
    url   = url("https://github.com/belgacea")
  )
)

// Versions

scalaVersion := "2.12.15"
val sparkVersion = "3.2.0"

// Resolvers

resolvers += "bintray-spark-packages" at "https://repos.spark-packages.org"

// Dependencies

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion  % Provided,
  "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,
  "org.apache.logging.log4j" % "log4j-core" % "2.14.1",
  "com.holdenkarau" %% "spark-testing-base" % "3.2.0_1.1.1" % "test",
  "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0" % "test"
)

// Plugins

enablePlugins(GitPlugin, ReleasePlugin, AutoVersionPlugin)

// Docs

val docScalacOptions = Seq("-groups", "-implicits", "-no-link-warnings", "-diagrams", "-diagrams-debug")
ThisBuild / doc / scalacOptions ++= docScalacOptions
ThisBuild / doc / autoAPIMappings := true

// Test

Test / testOptions += Tests.Argument("-oF")
Test / fork := true
Test / parallelExecution := false
Test / javaOptions ++= Seq("-Xms512M", "-Xmx2048M")

// TODOs
// Assembly
// Packaging
// Deployment

// Release

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,              // : ReleaseStep
  inquireVersions,                        // : ReleaseStep
  runClean,                               // : ReleaseStep
  runTest,                                // : ReleaseStep
  setReleaseVersion,                      // : ReleaseStep
  commitReleaseVersion,                   // : ReleaseStep, performs the initial git checks
  tagRelease,                             // : ReleaseStep
  publishArtifacts,                       // : ReleaseStep, checks whether `publishTo` is properly set up
  setNextVersion,                         // : ReleaseStep
  commitNextVersion,                      // : ReleaseStep
  pushChanges                             // : ReleaseStep, also checks that an upstream branch is properly configured
)