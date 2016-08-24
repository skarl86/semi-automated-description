name := "semi-automated-description-generator-system"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.0"
libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.4.0"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.6.0"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6"
// https://mvnrepository.com/artifact/com.github.ansell.pellet/pellet-owlapiv3
libraryDependencies += "com.github.ansell.pellet" % "pellet-owlapiv3" % "2.3.3"
// https://mvnrepository.com/artifact/com.hermit-reasoner/org.semanticweb.hermit
libraryDependencies += "com.hermit-reasoner" % "org.semanticweb.hermit" % "1.3.8.4"