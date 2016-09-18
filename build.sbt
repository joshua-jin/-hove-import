name := "hove-import"

version := "1.0"

scalaVersion := "2.11.8"

resolvers ++= Seq("RoundEights" at "http://maven.spikemark.net/roundeights")

libraryDependencies ++=  Seq(
  "org.apache.hbase" % "hbase-shaded-client" % "1.1.5",
  "com.roundeights" %% "hasher" % "1.2.0"
)

test in assembly := {}
assemblyJarName in assembly := "hove-import.jar"
mainClass in assembly := Some("cn.finance.hove.dataimport.Main")
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.last
}