import sbt._
import Keys._

object SparkSeqBuild extends Build {

    lazy val root = Project(id = "sparkseq", base = file(".")) aggregate(core, repl) dependsOn(core, repl)
	//		    settings (ScctPlugin.mergeReportSettings: _*) aggregate(core, repl)

    lazy val core = Project(id = "sparkseq-core", base = file("sparkseq-core"))
	//		    settings (ScctPlugin.instrumentSettings: _*)

    lazy val repl = Project(id = "sparkseq-repl", base = file("sparkseq-repl"))
	//		    settings (ScctPlugin.instrumentSettings: _*)
}
