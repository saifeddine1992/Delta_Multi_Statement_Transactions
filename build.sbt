name := "Multi_Statement"

version := "0.1"

scalaVersion := "2.12.10"
val sparkVersion = "3.1.1"
val deltaVersion = "1.0.0"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.1.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided"

libraryDependencies += "io.delta" %% "delta-core" % "1.0.0" % "provided"


libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1" % Test classifier "tests"
libraryDependencies += "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests"
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % Test classifier "tests"