name := "user-user-cf"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0-M15"

libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.3.0"
