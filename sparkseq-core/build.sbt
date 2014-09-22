import AssemblyKeys._ 

import scala.util.Properties

name := "sparkseq-core"

organization := "pl.edu.pw.elka"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.4"

publishTo := Some(Resolver.file("file", new File("/var/www/maven.sparkseq001.cloudapp.net/html/maven")))

val DEFAULT_HADOOP_VERSION = "1.2.1"
lazy val hadoopVersion = Properties.envOrElse("SPARK_HADOOP_VERSION", DEFAULT_HADOOP_VERSION)

ScctPlugin.instrumentSettings

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.1.0",
  "org.scalatest" % "scalatest_2.10" % "2.1.0-RC2" % "test",
  "org.apache.commons" % "commons-math3" % "3.2",
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
  "org.seqdoop" % "hadoop-bam" % "7.0.0",
  "org.seqdoop" % "htsjdk" % "1.118",
  //"picard" % "picard" % "1.93",
  //"samtools" % "samtools" % "1.93",
  //"tribble" % "tribble" % "1.93",
  //"variant" % "variant" % "1.93",
  "com.github.nscala-time" %% "nscala-time" % "0.8.0"
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Scala Tools Snapshots" at "http://scala-tools.org/repo-snapshots/",
  "ScalaNLP Maven2" at "http://repo.scalanlp.org/repo",
  "Spray" at "http://repo.spray.cc",
  "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
  "Hadoop-BAM" at "http://hadoop-bam.sourceforge.net/maven/"
)

testOptions in Test <+= (target in Test) map {
  t => Tests.Argument(TestFrameworks.ScalaTest, "junitxml(directory=\"%s\")" format (t / "test-reports"))
}

assemblySettings

test in assembly := {}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first 
    case PathList("fi", "tkk", "ics", xs @ _*) => MergeStrategy.first
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.first
    case PathList("org", "objectweb", xs @ _*)         => MergeStrategy.last
    case PathList("javax", "xml", xs @ _*)         => MergeStrategy.first
    case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
    case PathList("javax", "activation",xs @ _*)         => MergeStrategy.first
    case PathList("javax", "transaction",xs @ _*)         => MergeStrategy.first
    case PathList("javax", "mail", xs @ _*)     => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
    case "application.conf" => MergeStrategy.concat
    //case "META-INF/ECLIPSEF.RSA"     => MergeStrategy.discard
    case "META-INF/mimetypes.default"  => MergeStrategy.first
    case ("META-INF/ECLIPSEF.RSA") => MergeStrategy.first
    case ("META-INF/mailcap") => MergeStrategy.first
    case ("plugin.properties") => MergeStrategy.first
    case x => old(x)
  }
}

artifact in (Compile, assembly) ~= { art =>
  art.copy(`classifier` = Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)
