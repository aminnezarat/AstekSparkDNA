import scala.util.Properties

name := "sparkseq"

version := "0.1"

scalaVersion := "2.10.3"

ScctPlugin.instrumentSettings

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "2.1.0-RC2" % "test"
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
