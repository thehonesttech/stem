addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.2")

addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.0")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "2.1.1")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.13")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.7")
//addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.9.26")

val zioGrpcVersion = "0.5.0"
libraryDependencies ++= Seq("com.thesamet.scalapb" %% "compilerplugin" % "0.11.1",
"com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % zioGrpcVersion)