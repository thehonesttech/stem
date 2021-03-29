import sbtrelease.ReleaseStateTransformations.{checkSnapshotDependencies, commitNextVersion, commitReleaseVersion, inquireVersions, publishArtifacts, pushChanges, runClean, runTest, setNextVersion, setReleaseVersion, tagRelease}
import sbtrelease.Version.Bump

val grpcVersion = "1.34.0"

//inThisBuild(
//   List(
//     scalaVersion := "2.13.3",
//   semanticdbEnabled := true,
//   semanticdbVersion := scalafixSemanticdb.revision
//   )
// )


//ThisBuild / scalafixDependencies += "com.timushev" %% "zio-magic-comments" % "0.1.0"

lazy val commonProtobufSettings = Seq(
  PB.protoSources.in(Compile) := Seq(
    baseDirectory.value / "src/schemas/protobuf"
  ),
  PB.targets in Compile := Seq(
    scalapb.gen(grpc = true, flatPackage = false) -> (sourceManaged in Compile).value,
    scalapb.zio_grpc.ZioCodeGenerator -> (sourceManaged in Compile).value
  )
)


lazy val commonSettings = Seq(
  scalacOptions += "-Xsource:2.13",
  parallelExecution in Test := false,
  zioTest
)

lazy val noPublishSettings = Seq(publish := (()), publishLocal := (()), publishArtifact := false)

lazy val publishSettings = Seq(
  releaseCrossBuild := true,
  releaseVersionBump := Bump.Minor,
  releaseCommitMessage := s"Set version to ${
    if (releaseUseGlobalVersion.value) (version in ThisBuild).value
    else version.value
  }",
  releaseIgnoreUntrackedFiles := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  homepage := Some(url("https://github.com/thehonesttech/stem")),
  licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT")),
  publishMavenStyle := true,
  publishArtifact in Test := false,
  resolvers ++= Seq(Resolver.sonatypeRepo("releases"), Resolver.sonatypeRepo("snapshots")),
  pomIncludeRepository := { _ =>
    false
  },
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "content/repositories/releases")
  },
  autoAPIMappings := true,
  scmInfo := Some(
    ScmInfo(url("https://github.com/thehonesttech/stem"), "scm:git:git@github.com:thehonesttech/stem.git")
  ),
  pomExtra :=
    <developers>
      <developer>
        <id>thehonesttech</id>
        <name>Tobia Loschiavo</name>
        <url>https://github.com/thehonesttech/stem</url>
      </developer>
    </developers>
)

lazy val credentialSettings = Seq(
  // For Travis CI - see http://www.cakesolutions.net/teamblogs/publishing-artefacts-to-oss-sonatype-nexus-using-sbt-and-travis-ci
  credentials ++= (for {
    username <- Option(System.getenv().get("SONATYPE_USERNAME"))
    password <- Option(System.getenv().get("SONATYPE_PASSWORD"))
  } yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq
)


def stemModule(id: String, path: String, description: String): Project =
  Project(id, file(path))
    .settings(moduleName := id, name := description)

lazy val `core` = stemModule("stem-core", "core", "Core framework")
  .dependsOn(`data`, `readside`, `macros`)
  .settings(libraryDependencies ++= allDeps)
  .settings(commonProtobufSettings)
  .settings(publishSettings)
lazy val `data` = stemModule("stem-data", "data", "Data structures").settings(libraryDependencies ++= allDeps)
  .settings(publishSettings)
lazy val `readside` =
  stemModule("steam-readside", "readside", "Read side views").dependsOn(`data`).settings(libraryDependencies ++= allDeps).settings(commonProtobufSettings)
    .settings(publishSettings)
lazy val `macros` = stemModule("steam-macros", "macros", "Protocol macros").dependsOn(`data`).settings(libraryDependencies ++= allDeps)
  .settings(publishSettings)
lazy val `example` = stemModule("example", "example", "Ledger example")
  .dependsOn(`core`, `macros`, `readside`)
  .settings(libraryDependencies ++= testDeps ++ exampleDeps)
  .settings(noPublishSettings)
  .settings(zioTest)
  .settings(commonProtobufSettings)


lazy val root = (project in file("."))
  .settings(
    inThisBuild(
      List(
        organization := "io.github.thehonesttech",
        scalaVersion := "2.13.3",
        version := "0.1.1-SNAPSHOT"
      )
    ),
    name := "Stem",
    commonSettings
  ).settings(noPublishSettings)

val exampleDeps = Seq(
  "com.vladkopanev" %% "zio-saga-core" % "0.4.0"
)

val testDeps = Seq(
  "org.scalatest" %% "scalatest" % "3.1.1" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test,
  "org.scalatestplus" %% "scalacheck-1-14" % "3.2.0.0" % Test,
  "com.github.chocpanda" %% "scalacheck-magnolia" % "0.4.0" % Test,
  "dev.zio" %% "zio-test" % "1.0.5",
  "dev.zio" %% "zio-test-sbt" % "1.0.5" % Test,
  "dev.zio" %% "zio-test-magnolia" % "1.0.5" % Test
)

val allDeps = Seq(
  "org.apache.kafka" % "kafka-clients" % "2.1.0",
  "com.typesafe.akka" %% "akka-cluster-sharding" % "2.5.32",
  "com.typesafe.akka" %% "akka-cluster" % "2.5.32",
  "dev.zio" %% "zio-streams" % "1.0.5",
  "dev.zio" %% "zio-kafka" % "0.14.0",
  "io.suzaku" %% "boopickle" % "1.3.3",
  "com.vladkopanev" %% "zio-saga-core" % "0.4.0",
  "org.scodec" %% "scodec-bits" % "1.1.24",
  "org.scodec" %% "scodec-core" % "1.11.7",
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
  "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.10" % "1.18.0-0" % "protobuf",
  "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.10" % "1.18.0-0",
  "io.github.kitlangton" %% "zio-magic" % "0.2.0",
  "io.grpc" % "grpc-netty" % grpcVersion,
) ++ testDeps

aggregateProjects(`core`, `example`, `data`, `readside`, `macros`)

lazy val sharedReleaseProcess = Seq(
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    publishArtifacts,
    setNextVersion,
    commitNextVersion,
    ReleaseStep(action = "sonatypeReleaseAll" :: _),
    pushChanges
  )
)

val zioTest = testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")