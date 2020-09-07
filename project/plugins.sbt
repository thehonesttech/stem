addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.34")

addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.0")

val zioGrpcVersion = "0.4.0"
libraryDependencies ++= Seq("com.thesamet.scalapb" %% "compilerplugin" % "0.10.8",
"com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % zioGrpcVersion)