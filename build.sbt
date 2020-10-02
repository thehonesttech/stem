val grpcVersion = "1.30.2"

lazy val commonProtobufSettings = Seq(
  PB.protoSources.in(Compile) := Seq(
    baseDirectory.value / "src/schemas/protobuf"
  ),
  PB.targets in Compile := Seq(
    scalapb.gen(grpc = true, flatPackage = false) -> (sourceManaged in Compile).value,
    scalapb.zio_grpc.ZioCodeGenerator             -> (sourceManaged in Compile).value
  )
)

lazy val commonSettings = Seq(
  scalacOptions += "-Xsource:2.13",
  parallelExecution in Test := false,
  zioTest
)

def stemModule(id: String, description: String): Project =
  Project(id, file(s"$id"))
    .settings(moduleName := id, name := description)

lazy val `core` = stemModule("core", "Core framework")
  .dependsOn(`data`, `readside`, `macros`)
  .settings(libraryDependencies ++= allDeps)
  .settings(commonProtobufSettings)
lazy val `data` = stemModule("data", "Data structures").settings(libraryDependencies ++= allDeps)
lazy val `readside` =
  stemModule("readside", "Read side views").dependsOn(`data`).settings(libraryDependencies ++= allDeps).settings(commonProtobufSettings)
lazy val `macros` = stemModule("macros", "Protocol macros").dependsOn(`data`).settings(libraryDependencies ++= allDeps)
lazy val `example` = stemModule("example", "Ledger example")
  .dependsOn(`core`, `macros`, `readside`)
  .settings(libraryDependencies ++= testDeps ++ exampleDeps)
  .settings(zioTest)
  .settings(commonProtobufSettings)


lazy val root = (project in file("."))
  .settings(
    inThisBuild(
      List(
        organization := "uk.co.thehonesttech",
        scalaVersion := "2.13.3",
        version := "0.1.0-SNAPSHOT"
      )
    ),
    name := "Stem",
    commonSettings
  )

val exampleDeps = Seq(
  "com.vladkopanev" %% "zio-saga-core" % "0.4.0"
)

val testDeps = Seq(
  "org.scalatest" %% "scalatest" % "3.1.1" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test,
  "org.scalatestplus" %% "scalacheck-1-14" % "3.2.0.0" % Test,
  "com.github.chocpanda" %% "scalacheck-magnolia" % "0.4.0" % Test,
  "dev.zio" %% "zio-test" % "1.0.1",
  "dev.zio" %% "zio-test-sbt" % "1.0.1" % Test,
  "dev.zio" %% "zio-test-magnolia" % "1.0.1" % Test
)

val allDeps = Seq(
  "org.apache.kafka" % "kafka-clients" % "2.1.0",
  "com.typesafe.akka" %% "akka-cluster-sharding" % "2.5.31",
  "com.typesafe.akka" %% "akka-cluster" % "2.5.31",
  "dev.zio" %% "zio-streams" % "1.0.0",
  "dev.zio" %% "zio-kafka" % "0.12.0",
  "io.suzaku" %% "boopickle" % "1.3.2",
  "com.vladkopanev" %% "zio-saga-core" % "0.4.0",
  "org.scodec" %% "scodec-bits" % "1.1.13",
  "org.scodec" %% "scodec-core" % "1.11.4",
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
  "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.10" % "1.18.0-0" % "protobuf",
  "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.10" % "1.18.0-0",
  "io.grpc" % "grpc-netty" % grpcVersion,
) ++ testDeps

aggregateProjects(`core`, `example`, `data`, `readside`, `macros`)

val zioTest = testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")