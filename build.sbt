name := "AATUDF"
version := "0.1"
scalaVersion := "2.11.8"
crossScalaVersions := Seq("2.11.8", "2.12.10")
libraryDependencies  ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided"
)
assemblyJarName in assembly := "aatudfs-1.3.4-spark-sql-2.4.3.jar"
test in assembly := {}