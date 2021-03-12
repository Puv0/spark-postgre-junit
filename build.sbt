name := "spark-jobs"

version := "0.1"

scalaVersion := "2.12.12"
val sparkVersion = "2.4.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.postgresql" % "postgresql" % "42.2.16",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.8.1" % Test,
  "junit" % "junit" % "4.12" % Test,
  "org.scalatest" %% "scalatest-funsuite" % "3.2.5" % "test"


)

fork in Test := true
