import sbt._

    object Plugins extends Build {
      val reporterPlugin = ProjectRef(new URI("git://github.com/mmarich/sbt-simple-junit-xml-reporter-plugin.git"),
        "sbt-simple-junit-xml-reporter-plugin")
      lazy val plugins = Project("plugins", file(".")).dependsOn(reporterPlugin)
    }
