addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.34")

addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.0")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "2.0.1")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.13")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.5")

val zioGrpcVersion = "0.4.0"
libraryDependencies ++= Seq("com.thesamet.scalapb" %% "compilerplugin" % "0.10.8",
"com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % zioGrpcVersion)