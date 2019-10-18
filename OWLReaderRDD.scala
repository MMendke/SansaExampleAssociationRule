package net.sansa_stack.examples.spark.owl

import net.sansa_stack.owl.spark.owl._
import org.apache.spark.sql.{SQLContext,SparkSession}
import org.semanticweb.owlapi.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object OWLReaderRDD {  
  val sparkContext = loadSparkContext()
  val sqlContext = loadSqlContext()
  var fileName : String = null
  def loadSparkContext(): SparkContext = 
  {
    @transient lazy val conf = new SparkConf().setAppName("Triple reader example") // replace with app name
    .set("spark.sql.crossJoin.enabled", "true").set("spark.driver.allowMultipleContexts","true")
    @transient lazy val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")
    sc
  }
  def loadSqlContext(): SQLContext = 
  {
    val context = new org.apache.spark.sql.SQLContext(sparkContext)
    import context.implicits._
    context    
  }
  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.syntax)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, syntax: String): Unit = {
    println(".============================================.")
    println("| Rules		| Confidence Value| Fitness Value |")
    println("============================================")

    val spark = SparkSession.builder
      .appName(s"OWL reader example ( $input + )($syntax)")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "net.sansa_stack.owl.spark.dataset.UnmodifiableCollectionKryoRegistrator")
      .getOrCreate()

    val rdd = syntax match {
      case "fun" => spark.owl(Syntax.FUNCTIONAL)(input)
      case "manch" => spark.owl(Syntax.MANCHESTER)(input)
      case "owl_xml" =>
        throw new RuntimeException("'" + syntax + "' - Not supported, yet.")
      case _ =>
        throw new RuntimeException("Invalid syntax type: '" + syntax + "'")
    }

    fileName =input.split("/").last.split(".owl")(0)
    val filteredRDD= rdd.filter(axiom => axiom match {
    case a: OWLObjectPropertyAssertionAxiom => true
    case _ => false})
    
    val propDetailRDD = rdd.filter(f=>(f.isInstanceOf[OWLObjectPropertyDomainAxiom]||f.isInstanceOf[OWLObjectPropertyRangeAxiom]))
    Preprocessing.run(spark,filteredRDD,propDetailRDD)
    spark.stop()
  }

  case class Config(
    in: String = "",
    syntax: String = "")

  // the CLI parser
  val parser = new scopt.OptionParser[Config]("RDD OWL reader example") {

    head("RDD OWL reader example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data")

    opt[String]('s', "syntax").required().valueName("{fun | manch | owl_xml}").
      action((x, c) => c.copy(syntax = x)).
      text("the syntax format")

    help("help").text("prints this usage text")
  }
}
