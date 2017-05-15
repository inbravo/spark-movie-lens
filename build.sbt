name := "spark-examples"

version := "1.0"

scalaVersion := "2.11.8"
val hadoopVersion = "2.7.1"
val sparkVersion = "2.1.0"

/* Spark */
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % sparkVersion

/* Hadoop */
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-api" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-client" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-common" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-server-common" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-server-web-proxy" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-server-nodemanager" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-shuffle" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-server-resourcemanager" % hadoopVersion

/* Msc for web layer */
libraryDependencies += "org.apache.curator" % "curator-framework" % "2.7.1"
libraryDependencies += "org.apache.curator" % "curator-recipes" % "2.7.1"
libraryDependencies += "javax.activation" % "activation" % "1.1"
libraryDependencies += "javax.servlet.jsp" % "javax.servlet.jsp-api" % "2.2.1"
libraryDependencies += "xerces" % "xercesImpl" % "2.9.1"
libraryDependencies += "com.jcraft" % "jsch" % "0.1.42"
libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1"
libraryDependencies += "com.sun.xml.bind" % "jaxb-impl" % "2.2.3"
libraryDependencies += "xml-apis" % "xml-apis" % "1.3.04"
libraryDependencies += "javax.xml.stream" % "stax-api" % "1.0-2"
libraryDependencies += "asm" % "asm" % "3.1"
libraryDependencies += "org.hsqldb" % "hsqldb" % "2.0.0"
libraryDependencies += "jline" % "jline" % "2.14"
libraryDependencies += "junit" % "junit" % "4.12"

/* Typesafe */
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "com.typesafe.scala-logging" % "scala-logging_2.11" % "3.5.0"
libraryDependencies += "com.typesafe.scala-logging" % "scala-logging-slf4j_2.11" % "2.1.2"

/* Scala */
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.8"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.8"
libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.11.8"
libraryDependencies += "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.7" % "test"

  	
/* Finagle */
libraryDependencies += "com.twitter" %% "finagle-http" % "6.42.0"
libraryDependencies += "com.github.finagle" %% "finch-core" % "0.13.0"
