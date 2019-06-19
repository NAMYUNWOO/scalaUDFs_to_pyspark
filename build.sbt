name := "AATUDF"
version := "0.1"
scalaVersion := "2.11.0"
libraryDependencies  ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided"
)
assemblyJarName in assembly := "aatudfs-1.0.0-spark-sql-2.4.3.jar"
test in assembly := {}