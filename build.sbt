name := "cqs-akka"

version := "0.1"

scalaVersion := "2.13.3"
lazy val akkaVersion = "2.9.5"
lazy val leveldbVersion = "0.7"
lazy val leveldbjniVersion = "1.8"
lazy val postgresVersion = "42.2.2"
lazy val cassandraVersion = "1.0.5"
lazy val json4sVersion = "3.2.11"
lazy val protobufVersion = "3.6.1"
lazy val scalikejdbc = "3.5.0"

scalacOptions += "-deprecation"
resolvers += "Akka library repository".at("https://repo.akka.io/maven")

libraryDependencies ++= Seq(

  "org.postgresql" % "postgresql" % postgresVersion,
  "org.scalikejdbc" %% "scalikejdbc" % scalikejdbc,

  "com.esri.geometry" % "esri-geometry-api" % "2.2.0",

  "com.typesafe.akka" %% "akka-persistence-cassandra" % cassandraVersion,
  "com.typesafe.akka" %% "akka-persistence-cassandra-launcher" % cassandraVersion,

  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-query" % akkaVersion,


  //  local levelDB stores
  "org.iq80.leveldb" % "leveldb" % leveldbVersion,
  "org.fusesource.leveldbjni" % "leveldbjni-all" % leveldbjniVersion,

  "com.lightbend.akka" %% "akka-stream-alpakka-slick" % "3.0.4",
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,

  "io.netty" % "netty-all" % "4.1.68.Final",
  "org.apache.tinkerpop" % "gremlin-core" % "3.4.10",

  "com.typesafe.akka" %% "akka-coordination" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
  "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion,
)

libraryDependencies += "com.typesafe" % "config" % "1.4.0"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"