name := "sparkseq-repl"

version := "0.1"

scalaVersion := "2.10.3"

ScctPlugin.instrumentSettings

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "0.9.0-incubating",
  "org.scalatest" % "scalatest_2.10" % "2.1.0-RC2" % "test",
  "com.github.nscala-time" %% "nscala-time" % "0.8.0"
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Scala Tools Snapshots" at "http://scala-tools.org/repo-snapshots/",
  "ScalaNLP Maven2" at "http://repo.scalanlp.org/repo",
  "Spray" at "http://repo.spray.cc",
  "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"
)

testOptions in Test <+= (target in Test) map {
  t => Tests.Argument(TestFrameworks.ScalaTest, "junitxml(directory=\"%s\")" format (t / "test-reports"))
}
