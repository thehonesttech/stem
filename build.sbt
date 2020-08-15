/** [[https://monix.io]] */
import scalapb.compiler.Version.scalapbVersion
val grpcVersion = "1.30.2"

PB.protoSources.in(Compile) := Seq(baseDirectory.value / "src/schemas/protobuf")
PB.targets in Compile := Seq(
  scalapb.gen(grpc = true) -> (sourceManaged in Compile).value,
  scalapb.zio_grpc.ZioCodeGenerator -> (sourceManaged in Compile).value,
)

lazy val commonSettings = Seq(
  scalacOptions += "-Xsource:2.13",
  addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3"),
  parallelExecution in Test := false,
)

def stemModule(id: String, description: String): Project =
  Project(id, file(s"modules/$id"))
    .settings(moduleName := id, name := description)


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
  libraryDependencies ++= Seq(
    "org.apache.kafka" % "kafka-clients" % "2.1.0",
    "dev.zio" %% "zio-akka-cluster" % "0.2.0",
    "dev.zio" %% "zio-streams" % "1.0.0",
    "dev.zio" %% "zio-kafka" % "0.12.0",
    "dev.zio" %% "zio-config" % "1.0.0-RC26",
    "dev.zio" %% "zio-config-magnolia" % "1.0.0-RC26",
    "com.sksamuel.avro4s" %% "avro4s-core" % "3.1.1",
    "io.suzaku" %% "boopickle" % "1.3.2",
    "dev.zio" %% "zio-logging" % "0.3.2",
    "org.typelevel" %% "cats-core" % "2.0.0",
    "org.scodec" %% "scodec-bits" % "1.1.13",
    "org.scodec" %% "scodec-core" % "1.11.4",
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
    "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.10" % "1.18.0-0" % "protobuf",
    "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.10" % "1.18.0-0",
    "io.grpc" % "grpc-netty" % grpcVersion
  ) ,  commonSettings)

