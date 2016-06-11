name := "BooliDownload"

version := "1.0"
scalaVersion := "2.11.8"
lazy val playVersion = "2.4.6"

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= Seq(
//      "org.scalaj" %% "scalaj-http" % "2.3.0"
      "com.typesafe.play" %% "play-ws" % playVersion,
      "com.typesafe.play" %% "play-json" % playVersion
    )
  )
