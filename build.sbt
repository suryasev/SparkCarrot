name := "SparkCarrot"

organization := "com.salesforce.spark.carrot"

version := "0.1"

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-math" % "2.2",
  "org.specs2" %% "specs2-core" % "2.4.9" % "test",
  "org.specs2" %% "specs2-mock" % "2.4.9" % "test",
  "com.rabbitmq" % "amqp-client" % "3.5.4",
  "org.apache.avro" % "avro" % "1.7.7" exclude("org.mortbay.jetty", "servlet-api"),
  "org.apache.spark" %% "spark-streaming" % "1.3.1",
  "org.apache.spark" %% "spark-core" % "1.3.1",
  "com.twitter" %% "chill-avro" % "0.5.2"
)

scalacOptions in Test ++= Seq("-Yrangepos")

